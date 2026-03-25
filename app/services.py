from datetime import date, datetime, timedelta, timezone
import math
import re
import time
from typing import Dict, List, Optional, Tuple
from urllib import request as urlrequest

import pandas as pd
import yfinance as yf
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.importers import decode_base64_document, parse_xlsx_holdings
from app.models import (
    Holding,
    ImportedHolding,
    ImportedPortfolioSnapshot,
    ImportedHoldingTransaction,
    RecurringSip,
    PortfolioSnapshot,
    Price,
    Transaction,
)
from app.schemas import (
    HoldingsImportPayload,
    ImportedHoldingTransactionCreate,
    RecurringSipCreate,
    TransactionCreate,
)

EPSILON = 1e-9
TRADING_DAYS_PER_YEAR = 252
RISK_FREE_RATE_ANNUAL = 0.05
AMFI_NAV_URL = "https://www.amfiindia.com/spages/NAVAll.txt"
AMFI_NAV_CACHE_TTL_SECONDS = 60 * 30
_AMFI_NAV_CACHE: Dict[str, object] = {
    "loaded_at": 0.0,
    "by_isin": {},
    "rows": [],
}


def _normalize_symbol(symbol: str) -> str:
    return symbol.strip().upper()


def _safe_number(value, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    if not math.isfinite(number):
        return default
    return number


def _coalesce_text(*values: object) -> Optional[str]:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _fetch_latest_close(symbol: str) -> Optional[float]:
    try:
        history = yf.Ticker(symbol).history(period="5d")
    except Exception:
        return None

    if history.empty or "Close" not in history:
        return None

    closes = history["Close"].dropna()
    if closes.empty:
        return None

    latest = float(closes.iloc[-1])
    if not math.isfinite(latest) or latest <= 0:
        return None

    return latest


def _upsert_portfolio_snapshot(
    db: Session,
    snapshot_date: date,
    portfolio_data: Dict[str, float],
    overwrite: bool = False,
) -> bool:
    existing_snapshot = db.query(PortfolioSnapshot).filter(
        PortfolioSnapshot.date == snapshot_date
    ).first()

    if existing_snapshot:
        if not overwrite:
            return False
        existing_snapshot.total_value = portfolio_data["total_current_value"]
        existing_snapshot.total_invested = portfolio_data["total_invested"]
        existing_snapshot.pnl = portfolio_data["total_pnl"]
    else:
        db.add(
            PortfolioSnapshot(
                total_value=portfolio_data["total_current_value"],
                total_invested=portfolio_data["total_invested"],
                pnl=portfolio_data["total_pnl"],
                date=snapshot_date,
            )
        )

    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        return False

    return True


def _add_months(base_date: date, months: int = 1) -> date:
    month_index = base_date.month - 1 + months
    year = base_date.year + month_index // 12
    month = month_index % 12 + 1
    month_lengths = [31, 29 if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0) else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    day = min(base_date.day, month_lengths[month - 1])
    return date(year, month, day)


def _build_daily_returns(snapshots: List[PortfolioSnapshot]) -> Dict[date, float]:
    daily_returns: Dict[date, float] = {}

    for i in range(1, len(snapshots)):
        prev = snapshots[i - 1].total_value
        current = snapshots[i].total_value

        if prev is None or prev <= 0 or current is None:
            continue

        daily_return = (current - prev) / prev
        if math.isfinite(daily_return):
            daily_returns[snapshots[i].date] = daily_return

    return daily_returns


def _build_daily_returns_from_rows(snapshots) -> Dict[date, float]:
    return _build_daily_returns(list(snapshots))


def _get_holding_by_symbol(db: Session, symbol: str) -> Optional[Holding]:
    return db.query(Holding).filter(func.upper(Holding.symbol) == symbol).first()


def _apply_transaction_to_holdings(
    db: Session, symbol: str, quantity: float, price: float, txn_type: str
) -> None:
    holding = _get_holding_by_symbol(db, symbol)

    if txn_type == "BUY":
        if not holding:
            db.add(Holding(symbol=symbol, quantity=quantity, avg_price=price))
            return

        existing_qty = _safe_number(holding.quantity)
        existing_avg = _safe_number(holding.avg_price)
        new_qty = existing_qty + quantity
        if new_qty <= 0:
            raise ValueError(f"Invalid resulting quantity for {symbol}")
        new_avg = ((existing_qty * existing_avg) + (quantity * price)) / new_qty
        holding.quantity = new_qty
        holding.avg_price = new_avg
        return

    if txn_type == "SELL":
        if not holding:
            raise ValueError(f"No holdings available to sell for {symbol}")

        existing_qty = _safe_number(holding.quantity)
        if quantity > existing_qty:
            raise ValueError(
                f"Sell quantity {quantity} exceeds available holdings {existing_qty} for {symbol}"
            )

        remaining_qty = existing_qty - quantity
        if remaining_qty <= EPSILON:
            db.delete(holding)
        else:
            holding.quantity = remaining_qty
        return

    raise ValueError(f"Unsupported transaction type: {txn_type}")


def update_prices(db: Session):
    holdings = db.query(Holding).all()
    today = date.today()

    symbols = sorted({
        _normalize_symbol(h.symbol)
        for h in holdings
        if h.symbol and h.symbol.strip()
    })

    existing_symbols = set()
    if symbols:
        existing_symbols = {
            row[0]
            for row in db.query(func.upper(Price.symbol)).filter(
                Price.date == today,
                func.upper(Price.symbol).in_(symbols)
            ).all()
        }

    updated_prices = 0
    failed_symbols: List[str] = []

    for symbol in symbols:
        if symbol in existing_symbols:
            continue

        current_price = _fetch_latest_close(symbol)
        if current_price is None:
            failed_symbols.append(symbol)
            continue

        db.add(Price(symbol=symbol, price=current_price, date=today))
        updated_prices += 1

    try:
        db.commit()
    except IntegrityError:
        db.rollback()

    portfolio_data = calculate_portfolio_value(db, as_of=today)
    snapshot_created = _upsert_portfolio_snapshot(db, today, portfolio_data)

    return {
        "updated_prices": updated_prices,
        "failed_symbols": failed_symbols,
        "snapshot_created": snapshot_created,
    }


def calculate_portfolio_value(db: Session, as_of: Optional[date] = None):
    holdings = db.query(Holding).all()
    today = as_of or date.today()

    total_invested = 0.0
    total_current_value = 0.0
    missing_price_symbols: List[str] = []
    stale_price_symbols: List[str] = []

    for h in holdings:
        quantity = _safe_number(h.quantity)
        avg_price = _safe_number(h.avg_price)
        invested = quantity * avg_price

        if not h.symbol or not str(h.symbol).strip():
            missing_price_symbols.append("INVALID_SYMBOL")
            continue

        symbol = _normalize_symbol(str(h.symbol))
        price_record = db.query(Price).filter(
            func.upper(Price.symbol) == symbol,
            Price.date <= today
        ).order_by(Price.date.desc()).first()

        if not price_record:
            missing_price_symbols.append(symbol)
            continue

        if price_record.date != today:
            stale_price_symbols.append(symbol)

        total_invested += invested
        total_current_value += quantity * _safe_number(price_record.price)

    total_pnl = total_current_value - total_invested

    return {
        "total_invested": round(total_invested, 2),
        "total_current_value": round(total_current_value, 2),
        "total_pnl": round(total_pnl, 2),
        "missing_price_symbols": sorted(set(missing_price_symbols)),
        "stale_price_symbols": sorted(set(stale_price_symbols)),
    }


def calculate_performance_metrics(db: Session):
    snapshots = db.query(PortfolioSnapshot).order_by(PortfolioSnapshot.date).all()

    if len(snapshots) < 2:
        return {"message": "Not enough data for performance calculation"}

    start_value = snapshots[0].total_value
    latest_value = snapshots[-1].total_value

    if start_value is None or start_value <= 0:
        return {"message": "Start value must be greater than zero"}

    if latest_value is None:
        return {"message": "Latest portfolio value is missing"}

    absolute_return_percent = ((latest_value - start_value) / start_value) * 100

    return {
        "start_value": start_value,
        "latest_value": latest_value,
        "absolute_return_percent": round(absolute_return_percent, 2),
    }


def calculate_daily_returns(db: Session, limit: Optional[int] = None):
    snapshots = db.query(PortfolioSnapshot).order_by(PortfolioSnapshot.date.asc()).all()

    if len(snapshots) < 2:
        return {"message": "Not enough data for daily return calculation"}

    daily_returns_by_date = _build_daily_returns(snapshots)
    if not daily_returns_by_date:
        return {"message": "Not enough valid return observations"}

    rows = [
        {
            "date": snapshot_date.isoformat(),
            "daily_return": round(daily_return, 6),
            "daily_return_percent": round(daily_return * 100, 4),
        }
        for snapshot_date, daily_return in sorted(daily_returns_by_date.items())
    ]

    if limit is not None:
        rows = rows[-limit:]

    return {
        "observations": len(rows),
        "daily_returns": rows,
    }


def calculate_max_drawdown(db: Session):
    snapshots = db.query(PortfolioSnapshot).order_by(PortfolioSnapshot.date).all()

    if len(snapshots) < 2:
        return {"message": "Not enough data for drawdown calculation"}

    peak: Optional[float] = None
    max_drawdown = 0.0

    for snapshot in snapshots:
        value = snapshot.total_value
        if value is None:
            continue

        if peak is None:
            if value <= 0:
                continue
            peak = value
            continue

        if value > peak:
            peak = value

        drawdown = (value - peak) / peak
        if drawdown < max_drawdown:
            max_drawdown = drawdown

    if peak is None:
        return {"message": "Not enough positive values for drawdown calculation"}

    return {
        "max_drawdown_percent": round(max_drawdown * 100, 2)
    }


def calculate_volatility(db: Session):
    snapshots = db.query(PortfolioSnapshot).order_by(PortfolioSnapshot.date).all()

    if len(snapshots) < 2:
        return {"message": "Not enough data for volatility calculation"}

    daily_returns = list(_build_daily_returns(snapshots).values())
    if len(daily_returns) < 2:
        return {"message": "Not enough valid return observations"}

    mean_return = sum(daily_returns) / len(daily_returns)
    variance = sum((r - mean_return) ** 2 for r in daily_returns) / (len(daily_returns) - 1)
    volatility = math.sqrt(variance)

    return {
        "volatility_percent": round(volatility * 100, 2),
        "observations": len(daily_returns),
    }


def calculate_sharpe_ratio(db: Session):
    snapshots = db.query(PortfolioSnapshot).order_by(PortfolioSnapshot.date).all()

    if len(snapshots) < 2:
        return {"message": "Not enough data for Sharpe ratio calculation"}

    daily_returns = list(_build_daily_returns(snapshots).values())
    if len(daily_returns) < 2:
        return {"message": "Not enough valid return observations"}

    mean_return = sum(daily_returns) / len(daily_returns)
    variance = sum((r - mean_return) ** 2 for r in daily_returns) / (len(daily_returns) - 1)
    volatility = math.sqrt(variance)

    if volatility == 0:
        return {"message": "Volatility is zero, Sharpe ratio undefined"}

    risk_free_rate_daily = RISK_FREE_RATE_ANNUAL / TRADING_DAYS_PER_YEAR
    sharpe_ratio = ((mean_return - risk_free_rate_daily) / volatility) * math.sqrt(TRADING_DAYS_PER_YEAR)

    return {
        "sharpe_ratio": round(sharpe_ratio, 4),
        "observations": len(daily_returns),
    }


def calculate_rolling_volatility(db: Session, window: int = 3):
    if window < 2:
        return {"message": "Window must be at least 2"}

    snapshots = db.query(PortfolioSnapshot).order_by(PortfolioSnapshot.date).all()
    daily_returns = list(_build_daily_returns(snapshots).values())

    if len(daily_returns) < window:
        return {"message": "Not enough data for rolling calculation"}

    rolling_vol = []

    for i in range(window, len(daily_returns) + 1):
        window_slice = daily_returns[i - window:i]
        mean_return = sum(window_slice) / window
        variance = sum((r - mean_return) ** 2 for r in window_slice) / (window - 1)
        vol = math.sqrt(variance)
        rolling_vol.append(round(vol * 100, 2))

    return {
        "window": window,
        "rolling_volatility_percent": rolling_vol,
        "observations": len(daily_returns),
    }



def _get_aligned_return_series(db: Session, benchmark_symbol: str = "^NSEI"):
    """
    Returns two aligned pandas Series:
    portfolio_returns, benchmark_returns
    aligned by common dates.
    """
    snapshots = db.query(PortfolioSnapshot).order_by(PortfolioSnapshot.date).all()

    if len(snapshots) < 2:
        return None, None

    portfolio_returns_by_date = _build_daily_returns(snapshots)
    if len(portfolio_returns_by_date) < 2:
        return None, None

    start_date = min(portfolio_returns_by_date)
    end_date = max(portfolio_returns_by_date) + timedelta(days=1)

    try:
        benchmark_data = yf.download(
            benchmark_symbol,
            start=start_date,
            end=end_date,
            auto_adjust=True,
            progress=False,
        )
    except Exception:
        return None, None

    if benchmark_data.empty or "Close" not in benchmark_data:
        return None, None

    benchmark_close = benchmark_data["Close"]
    if isinstance(benchmark_close, pd.DataFrame):
        if benchmark_close.empty or benchmark_close.shape[1] == 0:
            return None, None
        benchmark_close = benchmark_close.iloc[:, 0]

    benchmark_close = pd.to_numeric(benchmark_close, errors="coerce").dropna()
    if benchmark_close.empty:
        return None, None

    benchmark_returns = benchmark_close.pct_change().dropna()
    if benchmark_returns.empty:
        return None, None

    benchmark_returns.index = pd.to_datetime(benchmark_returns.index).date

    portfolio_series = pd.Series(portfolio_returns_by_date, dtype="float64")
    benchmark_series = benchmark_returns.astype("float64")

    common_dates = sorted(
        set(portfolio_series.index).intersection(set(benchmark_series.index))
    )
    if len(common_dates) < 2:
        return None, None

    aligned_portfolio = portfolio_series.loc[common_dates]
    aligned_benchmark = benchmark_series.loc[common_dates]

    aligned = pd.concat([aligned_portfolio, aligned_benchmark], axis=1).dropna()
    if len(aligned) < 2:
        return None, None

    return aligned.iloc[:, 0], aligned.iloc[:, 1]


def _get_aligned_return_series_for_snapshots(
    snapshots,
    benchmark_symbol: str = "^NSEI",
):
    if len(snapshots) < 2:
        return None, None

    portfolio_returns_by_date = _build_daily_returns_from_rows(snapshots)
    if len(portfolio_returns_by_date) < 2:
        return None, None

    start_date = min(portfolio_returns_by_date)
    end_date = max(portfolio_returns_by_date) + timedelta(days=1)

    try:
        benchmark_data = yf.download(
            benchmark_symbol,
            start=start_date,
            end=end_date,
            auto_adjust=True,
            progress=False,
        )
    except Exception:
        return None, None

    if benchmark_data.empty or "Close" not in benchmark_data:
        return None, None

    benchmark_close = benchmark_data["Close"]
    if isinstance(benchmark_close, pd.DataFrame):
        if benchmark_close.empty or benchmark_close.shape[1] == 0:
            return None, None
        benchmark_close = benchmark_close.iloc[:, 0]

    benchmark_close = pd.to_numeric(benchmark_close, errors="coerce").dropna()
    if benchmark_close.empty:
        return None, None

    benchmark_returns = benchmark_close.pct_change().dropna()
    if benchmark_returns.empty:
        return None, None

    benchmark_returns.index = pd.to_datetime(benchmark_returns.index).date

    portfolio_series = pd.Series(portfolio_returns_by_date, dtype="float64")
    benchmark_series = benchmark_returns.astype("float64")

    common_dates = sorted(set(portfolio_series.index).intersection(set(benchmark_series.index)))
    if len(common_dates) < 2:
        return None, None

    aligned_portfolio = portfolio_series.loc[common_dates]
    aligned_benchmark = benchmark_series.loc[common_dates]
    aligned = pd.concat([aligned_portfolio, aligned_benchmark], axis=1).dropna()
    if len(aligned) < 2:
        return None, None

    return aligned.iloc[:, 0], aligned.iloc[:, 1]


def calculate_beta(db: Session, benchmark_symbol: str = "^NSEI"):
    aligned_portfolio, aligned_benchmark = _get_aligned_return_series(db, benchmark_symbol)

    if aligned_portfolio is None:
        return {"message": "Not enough overlapping dates with benchmark"}

    variance = float(aligned_benchmark.var(ddof=1))
    if not math.isfinite(variance) or variance == 0:
        return {"message": "Benchmark variance is zero"}

    covariance = float(aligned_portfolio.cov(aligned_benchmark))
    if not math.isfinite(covariance):
        return {"message": "Could not compute covariance"}

    beta = covariance / variance
    if not math.isfinite(beta):
        return {"message": "Could not compute beta"}

    return {
        "benchmark": benchmark_symbol,
        "beta": round(float(beta), 4),
        "observations": len(aligned_portfolio),
    }

def calculate_alpha(
    db: Session,
    benchmark_symbol: str = "^NSEI",
    risk_free_rate_annual: float = RISK_FREE_RATE_ANNUAL,
):
    aligned_portfolio, aligned_benchmark = _get_aligned_return_series(db, benchmark_symbol)

    if aligned_portfolio is None:
        return {"message": "Not enough overlapping dates with benchmark"}

    portfolio_mean = float(aligned_portfolio.mean())
    benchmark_mean = float(aligned_benchmark.mean())

    variance = float(aligned_benchmark.var(ddof=1))
    if not math.isfinite(variance) or variance == 0:
        return {"message": "Benchmark variance is zero"}

    covariance = float(aligned_portfolio.cov(aligned_benchmark))
    beta = covariance / variance

    trading_days = TRADING_DAYS_PER_YEAR
    risk_free_daily = risk_free_rate_annual / trading_days

    expected_return = risk_free_daily + beta * (benchmark_mean - risk_free_daily)
    alpha = portfolio_mean - expected_return

    return {
        "benchmark": benchmark_symbol,
        "alpha_daily": round(alpha, 6),
        "alpha_annualized": round(alpha * trading_days, 4),
        "beta": round(beta, 4),
        "observations": len(aligned_portfolio),
    }


def calculate_information_ratio(db: Session, benchmark_symbol: str = "^NSEI"):
    aligned_portfolio, aligned_benchmark = _get_aligned_return_series(db, benchmark_symbol)

    if aligned_portfolio is None:
        return {"message": "Not enough overlapping dates with benchmark"}

    active_returns = aligned_portfolio - aligned_benchmark
    if len(active_returns) < 2:
        return {"message": "Not enough valid return observations"}

    mean_active_return = float(active_returns.mean())
    tracking_error = float(active_returns.std(ddof=1))

    if not math.isfinite(tracking_error) or tracking_error == 0:
        return {"message": "Tracking error is zero"}

    information_ratio = mean_active_return / tracking_error
    if not math.isfinite(information_ratio):
        return {"message": "Could not compute information ratio"}

    trading_days = TRADING_DAYS_PER_YEAR
    return {
        "benchmark": benchmark_symbol,
        "information_ratio_daily": round(information_ratio, 6),
        "information_ratio_annualized": round(information_ratio * math.sqrt(trading_days), 4),
        "mean_active_return_daily": round(mean_active_return, 6),
        "tracking_error_daily": round(tracking_error, 6),
        "observations": len(active_returns),
    }


def calculate_tracking_error(db: Session, benchmark_symbol: str = "^NSEI"):
    aligned_portfolio, aligned_benchmark = _get_aligned_return_series(db, benchmark_symbol)

    if aligned_portfolio is None:
        return {"message": "Not enough overlapping dates with benchmark"}

    active_returns = aligned_portfolio - aligned_benchmark
    if len(active_returns) < 2:
        return {"message": "Not enough valid return observations"}

    tracking_error_daily = float(active_returns.std(ddof=1))
    if not math.isfinite(tracking_error_daily):
        return {"message": "Could not compute tracking error"}

    trading_days = TRADING_DAYS_PER_YEAR
    return {
        "benchmark": benchmark_symbol,
        "tracking_error_daily": round(tracking_error_daily, 6),
        "tracking_error_annualized": round(tracking_error_daily * math.sqrt(trading_days), 4),
        "observations": len(active_returns),
    }

def create_transaction(db: Session, txn: TransactionCreate):
    symbol = _normalize_symbol(txn.symbol)
    txn_type = txn.type.upper()
    quantity = _safe_number(txn.quantity)
    price = _safe_number(txn.price)

    transaction = Transaction(
        symbol=symbol,
        quantity=quantity,
        price=price,
        type=txn_type,
        date=txn.date or date.today()
    )

    try:
        db.add(transaction)
        _apply_transaction_to_holdings(db, symbol, quantity, price, txn_type)
        db.commit()
    except Exception:
        db.rollback()
        raise

    db.refresh(transaction)

    return transaction


def create_transactions(db: Session, txns: List[TransactionCreate]):
    transactions: List[Transaction] = []

    try:
        for txn in txns:
            symbol = _normalize_symbol(txn.symbol)
            txn_type = txn.type.upper()
            quantity = _safe_number(txn.quantity)
            price = _safe_number(txn.price)

            transaction = Transaction(
                symbol=symbol,
                quantity=quantity,
                price=price,
                type=txn_type,
                date=txn.date or date.today(),
            )
            transactions.append(transaction)
            db.add(transaction)
            _apply_transaction_to_holdings(db, symbol, quantity, price, txn_type)

        db.commit()
    except Exception:
        db.rollback()
        raise

    for transaction in transactions:
        db.refresh(transaction)

    return transactions

def calculate_holdings_from_transactions(db: Session, as_of: Optional[date] = None):
    cutoff = as_of or date.today()

    transactions = db.query(Transaction).filter(
        Transaction.date <= cutoff
    ).order_by(Transaction.date.asc(), Transaction.id.asc()).all()

    holdings: Dict[str, float] = {}
    negative_symbols: List[str] = []

    for txn in transactions:
        symbol = _normalize_symbol(txn.symbol) if txn.symbol else ""
        if not symbol:
            continue
        qty = _safe_number(txn.quantity)

        if txn.type == "BUY":
            holdings[symbol] = holdings.get(symbol, 0) + qty
        elif txn.type == "SELL":
            holdings[symbol] = holdings.get(symbol, 0) - qty
            if holdings[symbol] < -EPSILON:
                negative_symbols.append(symbol)

    # Keep only open long positions in the holdings map.
    # Negative positions are tracked separately for reconciliation/debugging.
    holdings = {s: q for s, q in holdings.items() if q > EPSILON}

    return {
        "holdings": holdings,
        "negative_symbols": sorted(set(negative_symbols)),
    }

def portfolio_value_from_ledger(db: Session, as_of: Optional[date] = None):
    ledger = calculate_holdings_from_transactions(db, as_of)
    holdings = ledger["holdings"]
    negative_symbols = ledger["negative_symbols"]

    total_value = 0
    missing_price_symbols: List[str] = []

    for symbol, quantity in holdings.items():
        price_record = db.query(Price).filter(
            func.upper(Price.symbol) == symbol,
            Price.date <= (as_of or date.today())
        ).order_by(Price.date.desc()).first()

        if price_record:
            total_value += quantity * price_record.price
        else:
            missing_price_symbols.append(symbol)

    return {
        "total_value": total_value,
        "missing_price_symbols": sorted(set(missing_price_symbols)),
        "negative_symbols": negative_symbols,
    }


def _first_finite(*values: Optional[float]) -> Optional[float]:
    for value in values:
        if value is None:
            continue
        try:
            number = float(value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(number):
            return number
    return None


def _normalize_asset_type(value: Optional[str]) -> str:
    normalized = (value or "").strip().upper()
    if normalized in {"MUTUAL FUND", "MUTUAL_FUND", "MF"}:
        return "MUTUAL_FUND"
    if normalized == "ETF":
        return "ETF"
    return "STOCK"


def _display_asset_type(value: str) -> str:
    if value == "MUTUAL_FUND":
        return "Mutual Fund"
    if value == "ETF":
        return "ETF"
    return "Stock"


def _is_mutual_fund_holding(holding: ImportedHolding) -> bool:
    if (holding.asset_type or "").strip().upper() == "MUTUAL_FUND":
        return True
    isin_text = (holding.isin or "").strip().upper()
    if isin_text.startswith("INF"):
        return True
    symbol_text = (holding.symbol or "").strip().upper()
    return "FUND" in symbol_text


def _normalize_fund_name(value: Optional[str]) -> str:
    text = (value or "").strip().upper()
    text = re.sub(r"\bPLAN\b", "", text)
    text = re.sub(r"\bDIRECT\b", "", text)
    text = re.sub(r"\bREGULAR\b", "", text)
    text = re.sub(r"\bGROWTH\b", "", text)
    text = re.sub(r"\bOPTION\b", "", text)
    return re.sub(r"[^A-Z0-9]", "", text)


def _load_amfi_nav_cache() -> Tuple[Dict[str, Dict[str, object]], List[Dict[str, object]]]:
    now = time.time()
    loaded_at = float(_AMFI_NAV_CACHE.get("loaded_at") or 0.0)
    if (
        now - loaded_at < AMFI_NAV_CACHE_TTL_SECONDS
        and _AMFI_NAV_CACHE.get("rows")
    ):
        return (
            _AMFI_NAV_CACHE.get("by_isin", {}),
            _AMFI_NAV_CACHE.get("rows", []),
        )

    by_isin: Dict[str, Dict[str, object]] = {}
    rows: List[Dict[str, object]] = []

    try:
        with urlrequest.urlopen(AMFI_NAV_URL, timeout=8) as response:
            payload = response.read().decode("utf-8", errors="ignore")
    except Exception:
        return (
            _AMFI_NAV_CACHE.get("by_isin", {}),
            _AMFI_NAV_CACHE.get("rows", []),
        )

    for line in payload.splitlines():
        parts = [part.strip() for part in line.split(";")]
        if len(parts) < 6:
            continue

        scheme_code, isin_growth, isin_reinvest, scheme_name, nav_text, nav_date = parts[:6]
        try:
            nav = float(nav_text.replace(",", ""))
        except (TypeError, ValueError):
            continue

        if not math.isfinite(nav) or nav <= 0:
            continue

        row = {
            "scheme_code": scheme_code,
            "scheme_name": scheme_name,
            "nav": nav,
            "date": nav_date,
            "name_key": _normalize_fund_name(scheme_name),
        }
        rows.append(row)

        for isin in (isin_growth, isin_reinvest):
            isin_key = (isin or "").strip().upper()
            if isin_key and isin_key.startswith("INF"):
                by_isin[isin_key] = row

    _AMFI_NAV_CACHE["loaded_at"] = now
    _AMFI_NAV_CACHE["by_isin"] = by_isin
    _AMFI_NAV_CACHE["rows"] = rows
    return by_isin, rows


def _fetch_mutual_fund_snapshot(holding: ImportedHolding) -> Dict[str, Optional[float] | str]:
    by_isin, rows = _load_amfi_nav_cache()
    isin_key = (holding.isin or "").strip().upper()
    matched = by_isin.get(isin_key)

    if matched is None:
        search_keys = [
            _normalize_fund_name(holding.symbol),
            _normalize_fund_name(holding.company_name),
        ]
        search_keys = [key for key in search_keys if key]
        for row in rows:
            name_key = str(row.get("name_key") or "")
            if any(key in name_key or name_key in key for key in search_keys):
                matched = row
                break

    if not matched:
        return {}

    nav = _first_finite(matched.get("nav"))
    if nav is None:
        return {}

    prev_close = _first_finite(holding.current_price, nav)

    return {
        "exchange_symbol": f"AMFI:{matched.get('scheme_code')}",
        "current_price": nav,
        "prev_close": prev_close,
        "company_name": str(matched.get("scheme_name") or holding.company_name or ""),
        "sector": "Mutual Fund",
        "geography": "India",
        "quote_type": "MUTUALFUND",
        "pe_ratio": None,
    }


def _candidate_market_symbols(holding: ImportedHolding) -> List[str]:
    if holding.exchange_symbol:
        return [holding.exchange_symbol]

    symbol = _normalize_symbol(holding.symbol or "")
    if not symbol:
        return []

    candidates = [symbol]
    if "." not in symbol:
        candidates.extend([f"{symbol}.NS", f"{symbol}.BO"])
    return candidates


def _fetch_quote_snapshot(holding: ImportedHolding) -> Dict[str, Optional[float] | str]:
    if _is_mutual_fund_holding(holding):
        mutual_fund_snapshot = _fetch_mutual_fund_snapshot(holding)
        if mutual_fund_snapshot:
            return mutual_fund_snapshot

    for candidate in _candidate_market_symbols(holding):
        try:
            ticker = yf.Ticker(candidate)
            history = ticker.history(period="5d", auto_adjust=False)
        except Exception:
            continue

        if history.empty or "Close" not in history:
            continue

        closes = history["Close"].dropna()
        if closes.empty:
            continue

        current_price = float(closes.iloc[-1])
        prev_close = float(closes.iloc[-2]) if len(closes) > 1 else current_price

        company_name = None
        sector = None
        geography = None
        quote_type = None
        pe_ratio = None

        try:
            info = ticker.info or {}
        except Exception:
            info = {}

        if info:
            company_name = info.get("shortName") or info.get("longName")
            sector = info.get("sector") or info.get("industry")
            geography = info.get("country")
            quote_type = info.get("quoteType")
            pe_ratio = _first_finite(
                info.get("trailingPE"),
                info.get("forwardPE"),
            )

        return {
            "exchange_symbol": candidate,
            "current_price": current_price,
            "prev_close": prev_close,
            "company_name": company_name,
            "sector": sector,
            "geography": geography,
            "quote_type": quote_type,
            "pe_ratio": pe_ratio,
        }

    return {}


def _fetch_close_for_date(holding: ImportedHolding, target_date: date) -> Optional[float]:
    for candidate in _candidate_market_symbols(holding):
        try:
            history = yf.Ticker(candidate).history(
                start=target_date,
                end=target_date + timedelta(days=7),
                auto_adjust=False,
            )
        except Exception:
            continue

        if history.empty or "Close" not in history:
            continue

        closes = pd.to_numeric(history["Close"], errors="coerce").dropna()
        if closes.empty:
            continue

        price = float(closes.iloc[0])
        if math.isfinite(price) and price > 0:
            return price

    return None


def import_holdings_workbook(db: Session, payload: HoldingsImportPayload):
    file_bytes = decode_base64_document(payload.content_base64)
    workbook = parse_xlsx_holdings(file_bytes)

    rows = workbook.rows
    if not rows:
        raise ValueError("No holding rows were found in the uploaded workbook")

    db.query(ImportedHolding).delete()

    created_rows = 0
    imported_at = datetime.utcnow()

    for row in rows:
        quantity = _safe_number(row.get("quantity"))
        avg_buy_cost = _safe_number(row.get("avg_buy_cost"))
        invested_amount = _safe_number(row.get("invested_amount")) or quantity * avg_buy_cost
        current_price = _first_finite(row.get("current_price"))
        prev_close = _first_finite(row.get("prev_close"))
        current_value = _first_finite(row.get("current_value"))

        if current_value is None and current_price is not None:
            current_value = quantity * current_price

        unrealized_pnl = _first_finite(row.get("unrealized_pnl"))
        if unrealized_pnl is None and current_value is not None:
            unrealized_pnl = current_value - invested_amount

        one_day_change = _first_finite(row.get("one_day_change"))
        if one_day_change is None and current_price is not None and prev_close is not None:
            one_day_change = (current_price - prev_close) * quantity

        db.add(
            ImportedHolding(
                symbol=_normalize_symbol(str(row.get("symbol", ""))),
                company_name=row.get("company_name"),
                isin=row.get("isin"),
                asset_type=_normalize_asset_type(str(row.get("asset_type"))),
                sector=row.get("sector"),
                quantity=quantity,
                avg_buy_cost=avg_buy_cost,
                invested_amount=invested_amount,
                prev_close=prev_close,
                current_price=current_price,
                current_value=current_value,
                one_day_change=one_day_change,
                unrealized_pnl=unrealized_pnl,
                currency=str(row.get("currency") or "INR"),
                source_file=payload.filename,
                imported_at=imported_at,
            )
        )
        created_rows += 1

    db.commit()
    refresh_imported_holdings_market_data(db)
    _upsert_imported_portfolio_snapshot(db, imported_at.date(), overwrite=True)

    return {
        "message": "Holdings imported successfully",
        "sheet_name": workbook.sheet_name,
        "rows_imported": created_rows,
        "source_file": payload.filename,
    }


def refresh_imported_holdings_market_data(db: Session):
    holdings = db.query(ImportedHolding).order_by(ImportedHolding.symbol.asc()).all()
    updated = 0
    failed_symbols: List[str] = []

    for holding in holdings:
        snapshot = _fetch_quote_snapshot(holding)
        quantity = _safe_number(holding.quantity)

        if not snapshot:
            failed_symbols.append(holding.symbol)
            continue

        current_price = _first_finite(snapshot.get("current_price"), holding.current_price)
        prev_close = _first_finite(snapshot.get("prev_close"), holding.prev_close)

        holding.exchange_symbol = _coalesce_text(snapshot.get("exchange_symbol"), holding.exchange_symbol)
        holding.company_name = _coalesce_text(snapshot.get("company_name"), holding.company_name)
        holding.sector = _coalesce_text(snapshot.get("sector"), holding.sector)
        holding.geography = _coalesce_text(snapshot.get("geography"), holding.geography, "India")
        holding.current_price = current_price
        holding.prev_close = prev_close
        holding.current_value = quantity * current_price if current_price is not None else holding.current_value
        if current_price is not None and prev_close is not None:
            holding.one_day_change = quantity * (current_price - prev_close)
        if holding.current_value is not None:
            holding.unrealized_pnl = holding.current_value - _safe_number(holding.invested_amount)
        holding.pe_ratio = _first_finite(snapshot.get("pe_ratio"), holding.pe_ratio)

        quote_type = str(snapshot.get("quote_type") or "").strip().upper()
        if quote_type == "ETF":
            holding.asset_type = "ETF"
        elif quote_type in {"MUTUALFUND", "MUTUAL FUND"}:
            holding.asset_type = "MUTUAL_FUND"

        updated += 1

    db.commit()
    snapshot_created = _upsert_imported_portfolio_snapshot(db)

    return {
        "message": "Imported holdings refreshed",
        "updated_count": updated,
        "failed_symbols": failed_symbols,
        "snapshot_created": snapshot_created,
    }


def _refresh_single_imported_holding(holding: ImportedHolding) -> None:
    snapshot = _fetch_quote_snapshot(holding)
    quantity = _safe_number(holding.quantity)

    current_price = _first_finite(
        snapshot.get("current_price") if snapshot else None,
        holding.current_price,
    )
    prev_close = _first_finite(
        snapshot.get("prev_close") if snapshot else None,
        holding.prev_close,
    )

    if snapshot:
        holding.exchange_symbol = _coalesce_text(snapshot.get("exchange_symbol"), holding.exchange_symbol)
        holding.company_name = _coalesce_text(snapshot.get("company_name"), holding.company_name)
        holding.sector = _coalesce_text(snapshot.get("sector"), holding.sector)
        holding.geography = _coalesce_text(snapshot.get("geography"), holding.geography, "India")
        holding.pe_ratio = _first_finite(snapshot.get("pe_ratio"), holding.pe_ratio)

        quote_type = str(snapshot.get("quote_type") or "").strip().upper()
        if quote_type == "ETF":
            holding.asset_type = "ETF"
        elif quote_type in {"MUTUALFUND", "MUTUAL FUND"}:
            holding.asset_type = "MUTUAL_FUND"

    holding.current_price = current_price
    holding.prev_close = prev_close
    holding.current_value = quantity * current_price if current_price is not None else quantity * _safe_number(holding.avg_buy_cost)
    if current_price is not None and prev_close is not None:
        holding.one_day_change = quantity * (current_price - prev_close)
    else:
        holding.one_day_change = 0.0
    holding.unrealized_pnl = _safe_number(holding.current_value) - _safe_number(holding.invested_amount)


def apply_imported_holding_transaction(db: Session, txn: ImportedHoldingTransactionCreate):
    symbol = _normalize_symbol(txn.symbol)
    txn_type = txn.type.upper()
    quantity = _safe_number(txn.quantity)
    price = _safe_number(txn.price)
    txn_date = txn.date or date.today()

    holding = db.query(ImportedHolding).filter(
        func.upper(ImportedHolding.symbol) == symbol
    ).first()

    if not holding:
        raise ValueError(f"No imported holding found for {symbol}")

    existing_qty = _safe_number(holding.quantity)
    existing_avg = _safe_number(holding.avg_buy_cost)
    existing_invested = _safe_number(holding.invested_amount)

    manual_txn = ImportedHoldingTransaction(
        symbol=symbol,
        quantity=quantity,
        price=price,
        type=txn_type,
        date=txn_date,
    )
    db.add(manual_txn)

    if txn_type == "BUY":
        new_qty = existing_qty + quantity
        if new_qty <= 0:
            raise ValueError(f"Invalid resulting quantity for {symbol}")
        new_invested = existing_invested + (quantity * price)
        holding.quantity = new_qty
        holding.invested_amount = round(new_invested, 2)
        holding.avg_buy_cost = round(new_invested / new_qty, 2)
        _refresh_single_imported_holding(holding)
        holding.imported_at = datetime.utcnow()
    elif txn_type == "SELL":
        if quantity > existing_qty:
            raise ValueError(
                f"Sell quantity {quantity} exceeds available holdings {existing_qty} for {symbol}"
            )

        remaining_qty = existing_qty - quantity
        cost_of_sold = quantity * existing_avg
        remaining_invested = max(existing_invested - cost_of_sold, 0.0)

        if remaining_qty <= EPSILON:
            db.delete(holding)
        else:
            holding.quantity = remaining_qty
            holding.invested_amount = round(remaining_invested, 2)
            holding.avg_buy_cost = round(remaining_invested / remaining_qty, 2) if remaining_qty else 0.0
            _refresh_single_imported_holding(holding)
            holding.imported_at = datetime.utcnow()
    else:
        raise ValueError(f"Unsupported transaction type: {txn_type}")

    try:
        db.commit()
    except Exception:
        db.rollback()
        raise

    _upsert_imported_portfolio_snapshot(
        db,
        snapshot_date=txn_date,
        allow_empty=True,
        overwrite=True,
    )
    if txn_date != date.today():
        _upsert_imported_portfolio_snapshot(
            db,
            snapshot_date=date.today(),
            allow_empty=True,
            overwrite=True,
        )

    return {
        "message": f"{txn_type.title()} transaction recorded for {symbol}",
        "symbol": symbol,
        "type": txn_type,
        "date": txn_date.isoformat(),
    }


def _serialize_imported_holding(holding: ImportedHolding) -> Dict[str, object]:
    invested_amount = _safe_number(holding.invested_amount)
    current_value = _safe_number(holding.current_value)
    one_day_change = _safe_number(holding.one_day_change)
    unrealized_pnl = _safe_number(holding.unrealized_pnl)
    weight_percent = 0.0

    return {
        "symbol": holding.symbol,
        "company_name": holding.company_name or holding.symbol,
        "isin": holding.isin,
        "asset_type": _display_asset_type(_normalize_asset_type(holding.asset_type)),
        "sector": holding.sector or "Unclassified",
        "geography": holding.geography or "India",
        "exchange_symbol": holding.exchange_symbol,
        "quantity": round(_safe_number(holding.quantity), 4),
        "avg_buy_cost": round(_safe_number(holding.avg_buy_cost), 2),
        "invested_amount": round(invested_amount, 2),
        "prev_close": round(_safe_number(holding.prev_close), 2) if holding.prev_close is not None else None,
        "current_price": round(_safe_number(holding.current_price), 2) if holding.current_price is not None else None,
        "current_value": round(current_value, 2),
        "one_day_change": round(one_day_change, 2),
        "unrealized_pnl": round(unrealized_pnl, 2),
        "pe_ratio": round(_safe_number(holding.pe_ratio), 2) if holding.pe_ratio is not None else None,
        "currency": holding.currency or "INR",
        "source_file": holding.source_file,
        "weight_percent": weight_percent,
    }


def _bucketize(rows: List[Dict[str, object]], field: str) -> List[Dict[str, float | str]]:
    totals: Dict[str, float] = {}
    grand_total = sum(_safe_number(row.get("current_value")) for row in rows)

    for row in rows:
        name = str(row.get(field) or "Unclassified").strip() or "Unclassified"
        totals[name] = totals.get(name, 0.0) + _safe_number(row.get("current_value"))

    buckets = [
        {
            "name": name,
            "value": round(value, 2),
            "weight_percent": round((value / grand_total) * 100, 2) if grand_total else 0.0,
        }
        for name, value in sorted(totals.items(), key=lambda item: item[1], reverse=True)
    ]
    return buckets


def _fetch_benchmark_summary(symbol: str = "^NSEI") -> Dict[str, object]:
    try:
        ticker = yf.Ticker(symbol)
        history = ticker.history(period="5d", auto_adjust=False)
    except Exception:
        return {
            "symbol": symbol,
            "name": "Nifty 50",
            "price": None,
            "prev_close": None,
            "one_day_change_percent": None,
            "pe_ratio": None,
        }

    closes = history["Close"].dropna() if not history.empty and "Close" in history else []
    daily_closes = pd.to_numeric(closes, errors="coerce").dropna() if len(closes) else pd.Series(dtype="float64")
    daily_dates = [pd.to_datetime(index).date() for index in daily_closes.index]

    try:
        fast_info = ticker.fast_info or {}
    except Exception:
        fast_info = {}

    try:
        info = ticker.info or {}
    except Exception:
        info = {}

    last_daily_close = float(daily_closes.iloc[-1]) if len(daily_closes) else None
    price = _first_finite(
        fast_info.get("regular_market_price"),
        fast_info.get("last_price"),
        info.get("regularMarketPrice"),
        info.get("currentPrice"),
        last_daily_close,
    )

    prev_close = _first_finite(
        fast_info.get("regular_market_previous_close"),
        fast_info.get("previous_close"),
        info.get("regularMarketPreviousClose"),
        info.get("previousClose"),
    )
    if (prev_close is None or prev_close <= 0) and len(daily_closes) >= 1:
        today_ist = datetime.now(timezone(timedelta(hours=5, minutes=30))).date()
        last_daily_date = daily_dates[-1]
        if len(daily_closes) >= 2:
            prev_close = float(daily_closes.iloc[-2]) if last_daily_date >= today_ist else float(daily_closes.iloc[-1])
        else:
            prev_close = float(daily_closes.iloc[-1])

    if prev_close is not None and prev_close <= 0:
        prev_close = None
    change_percent = None
    if price is not None and prev_close not in (None, 0):
        change_percent = ((price - prev_close) / prev_close) * 100

    pe_ratio = _first_finite(info.get("trailingPE"), info.get("forwardPE")) if info else None

    return {
        "symbol": symbol,
        "name": str(info.get("shortName") or "Nifty 50") if info else "Nifty 50",
        "price": round(price, 2) if price is not None else None,
        "prev_close": round(prev_close, 2) if prev_close is not None else None,
        "one_day_change_percent": round(change_percent, 2) if change_percent is not None else None,
        "pe_ratio": round(pe_ratio, 2) if pe_ratio is not None else None,
    }


def _fetch_bse_sensex_snapshot() -> Optional[Dict[str, float]]:
    try:
        request = urlrequest.Request(
            "https://m.bseindia.com/IndicesView.aspx",
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://m.bseindia.com/",
            },
        )
        with urlrequest.urlopen(request, timeout=6) as response:
            html = response.read().decode("utf-8", errors="ignore")
    except Exception:
        return None

    match = re.search(
        r"BSE SENSEX</a></td><td[^>]*>\s*([0-9,]+(?:\.[0-9]+)?)\s*</td>"
        r"<td[^>]*>\s*([+-]?[0-9,]+(?:\.[0-9]+)?)\s*</td>"
        r"<td[^>]*>\s*([+-]?[0-9,]+(?:\.[0-9]+)?)\s*</td>",
        html,
        flags=re.IGNORECASE,
    )
    if not match:
        return None

    try:
        current_level = float(match.group(1).replace(",", ""))
        points_change = float(match.group(2).replace(",", ""))
        change_percent = float(match.group(3).replace(",", ""))
    except (TypeError, ValueError):
        return None

    if not math.isfinite(current_level) or not math.isfinite(points_change):
        return None

    prev_close = current_level - points_change
    if not math.isfinite(prev_close) or prev_close <= 0:
        return None

    if not math.isfinite(change_percent):
        change_percent = ((points_change / prev_close) * 100)

    return {
        "current_level": current_level,
        "prev_close": prev_close,
        "points_change": points_change,
        "change_percent": change_percent,
    }


def _imported_portfolio_totals(holdings: List[ImportedHolding]) -> Dict[str, float]:
    total_current_value = 0.0
    total_invested = 0.0
    total_pnl = 0.0

    for holding in holdings:
        current_value = _safe_number(holding.current_value)
        invested_amount = _safe_number(holding.invested_amount)
        total_current_value += current_value
        total_invested += invested_amount
        total_pnl += current_value - invested_amount

    return {
        "total_current_value": round(total_current_value, 2),
        "total_invested": round(total_invested, 2),
        "total_pnl": round(total_pnl, 2),
    }


def _upsert_imported_holdings_snapshot(
    db: Session,
    snapshot_date: Optional[date] = None,
    allow_empty: bool = False,
    overwrite: bool = False,
) -> bool:
    holdings = db.query(ImportedHolding).all()
    if not holdings and not allow_empty:
        return False

    return _upsert_imported_portfolio_snapshot(
        db,
        snapshot_date or date.today(),
        allow_empty=allow_empty,
        overwrite=overwrite,
    )


def _upsert_imported_portfolio_snapshot(
    db: Session,
    snapshot_date: Optional[date] = None,
    allow_empty: bool = False,
    overwrite: bool = False,
) -> bool:
    holdings = db.query(ImportedHolding).all()
    if not holdings and not allow_empty:
        return False

    portfolio_data = (
        _imported_portfolio_totals(holdings)
        if holdings
        else {
            "total_current_value": 0.0,
            "total_invested": 0.0,
            "total_pnl": 0.0,
        }
    )

    target_date = snapshot_date or date.today()
    existing_snapshot = db.query(ImportedPortfolioSnapshot).filter(
        ImportedPortfolioSnapshot.date == target_date
    ).first()

    if existing_snapshot:
        if not overwrite:
            return False
        existing_snapshot.total_value = portfolio_data["total_current_value"]
        existing_snapshot.total_invested = portfolio_data["total_invested"]
        existing_snapshot.pnl = portfolio_data["total_pnl"]
    else:
        db.add(
            ImportedPortfolioSnapshot(
                total_value=portfolio_data["total_current_value"],
                total_invested=portfolio_data["total_invested"],
                pnl=portfolio_data["total_pnl"],
                date=target_date,
            )
        )

    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        return False

    return True


def _ensure_imported_snapshot_history(db: Session) -> None:
    existing_count = db.query(ImportedPortfolioSnapshot).count()
    if existing_count > 0:
        return

    latest_import = db.query(ImportedHolding).order_by(ImportedHolding.imported_at.desc()).first()
    import_cutoff = latest_import.imported_at.date() if latest_import and latest_import.imported_at else None

    query = db.query(PortfolioSnapshot).order_by(PortfolioSnapshot.date.asc())
    if import_cutoff:
        query = query.filter(PortfolioSnapshot.date >= import_cutoff)

    legacy_snapshots = query.all()
    if legacy_snapshots:
        for snapshot in legacy_snapshots:
            db.add(
                ImportedPortfolioSnapshot(
                    total_value=snapshot.total_value,
                    total_invested=snapshot.total_invested,
                    pnl=snapshot.pnl,
                    date=snapshot.date,
                )
            )
        try:
            db.commit()
            return
        except IntegrityError:
            db.rollback()

    _upsert_imported_portfolio_snapshot(db, allow_empty=True, overwrite=True)


def _fetch_benchmark_mini_chart(symbol: str, fallback_name: str) -> Dict[str, object]:
    try:
        ticker = yf.Ticker(symbol)
        intraday_history = ticker.history(period="1d", interval="1m", auto_adjust=False)
        if intraday_history.empty or "Close" not in intraday_history:
            intraday_history = ticker.history(period="1d", interval="5m", auto_adjust=False)
        if intraday_history.empty or "Close" not in intraday_history:
            intraday_history = ticker.history(period="2d", interval="15m", auto_adjust=False)
        daily_history = ticker.history(period="5d", auto_adjust=False)
    except Exception:
        return {
            "symbol": symbol,
            "name": fallback_name,
            "current_level": None,
            "prev_close": None,
            "points_change": None,
            "change_percent": None,
            "trend": None,
            "points": [],
        }

    if intraday_history.empty or "Close" not in intraday_history:
        return {
            "symbol": symbol,
            "name": fallback_name,
            "current_level": None,
            "prev_close": None,
            "points_change": None,
            "change_percent": None,
            "trend": None,
            "points": [],
        }

    closes = pd.to_numeric(intraday_history["Close"], errors="coerce").dropna()
    if closes.empty:
        return {
            "symbol": symbol,
            "name": fallback_name,
            "current_level": None,
            "prev_close": None,
            "points_change": None,
            "change_percent": None,
            "trend": None,
            "points": [],
        }

    intraday_dates = pd.to_datetime(closes.index).date
    latest_session_date = max(intraday_dates)
    closes = closes[[current_date == latest_session_date for current_date in intraday_dates]]
    if closes.empty:
        return {
            "symbol": symbol,
            "name": fallback_name,
            "current_level": None,
            "prev_close": None,
            "points_change": None,
            "change_percent": None,
            "trend": None,
            "points": [],
        }

    daily_closes = (
        pd.to_numeric(daily_history["Close"], errors="coerce").dropna()
        if not daily_history.empty and "Close" in daily_history
        else pd.Series(dtype="float64")
    )
    daily_dates = [pd.to_datetime(index).date() for index in daily_closes.index]

    intraday_last = float(closes.iloc[-1])
    try:
        fast_info = ticker.fast_info or {}
    except Exception:
        fast_info = {}
    try:
        info = ticker.info or {}
    except Exception:
        info = {}

    market_price = _first_finite(
        fast_info.get("regular_market_price"),
        fast_info.get("last_price"),
        info.get("regularMarketPrice"),
        info.get("currentPrice"),
    )
    market_prev_close = _first_finite(
        fast_info.get("regular_market_previous_close"),
        fast_info.get("previous_close"),
        info.get("regularMarketPreviousClose"),
        info.get("previousClose"),
    )
    market_change = _first_finite(
        info.get("regularMarketChange"),
        info.get("regularMarketChangePoint"),
    )
    market_change_percent = _first_finite(
        info.get("regularMarketChangePercent"),
    )

    bse_snapshot = _fetch_bse_sensex_snapshot() if symbol == "^BSESN" else None
    if bse_snapshot:
        current_level = bse_snapshot["current_level"]
        prev_close = bse_snapshot["prev_close"]
        points_change = bse_snapshot["points_change"]
        change_percent = bse_snapshot["change_percent"]
    elif (
        market_price is not None
        and market_change is not None
        and market_price > 0
    ):
        current_level = market_price
        points_change = market_change
        prev_close = current_level - points_change
        change_percent = (
            market_change_percent
            if market_change_percent is not None
            else (((current_level - prev_close) / prev_close) * 100 if prev_close not in (None, 0) else None)
        )
    else:
        current_level = _first_finite(market_price, intraday_last)
        if current_level is None or current_level <= 0:
            current_level = intraday_last

        prev_close = None
        if market_prev_close is not None and market_prev_close > 0:
            prev_close = market_prev_close
        elif len(daily_closes) >= 2 and daily_dates[-1] == latest_session_date:
            prev_close = float(daily_closes.iloc[-2])
        elif len(daily_closes) >= 1:
            prev_close = float(daily_closes.iloc[-1])
        elif len(closes) >= 1:
            prev_close = float(closes.iloc[0])

        points_change = current_level - prev_close if prev_close is not None else None
        change_percent = (
            ((current_level - prev_close) / prev_close) * 100
            if prev_close not in (None, 0)
            else None
        )

    adjustment = current_level - intraday_last
    adjusted_closes = closes + adjustment

    trend = None if points_change is None else ("positive" if points_change >= 0 else "negative")

    points = [
        {
            "date": pd.to_datetime(index).isoformat(),
            "value": round(float(value), 2),
        }
        for index, value in adjusted_closes.items()
    ]

    return {
        "symbol": symbol,
        "name": fallback_name,
        "current_level": round(current_level, 2),
        "prev_close": round(prev_close, 2) if prev_close is not None else None,
        "points_change": round(points_change, 2) if points_change is not None else None,
        "change_percent": round(change_percent, 2) if change_percent is not None else None,
        "trend": trend,
        "points": points,
    }


def _calculate_imported_risk_metrics(db: Session, benchmark_symbol: str = "^NSEI") -> Dict[str, object]:
    snapshots = db.query(ImportedPortfolioSnapshot).order_by(ImportedPortfolioSnapshot.date.asc()).all()
    daily_returns = list(_build_daily_returns_from_rows(snapshots).values())

    sharpe_ratio = None
    if len(daily_returns) >= 2:
        mean_return = sum(daily_returns) / len(daily_returns)
        variance = sum((r - mean_return) ** 2 for r in daily_returns) / (len(daily_returns) - 1)
        volatility = math.sqrt(variance)
        if volatility != 0:
            risk_free_daily = RISK_FREE_RATE_ANNUAL / TRADING_DAYS_PER_YEAR
            sharpe_ratio = round(
                ((mean_return - risk_free_daily) / volatility) * math.sqrt(TRADING_DAYS_PER_YEAR),
                4,
            )

    aligned_portfolio, aligned_benchmark = _get_aligned_return_series_for_snapshots(
        snapshots,
        benchmark_symbol,
    )
    beta = None
    alpha_annualized_percent = None
    observations = len(daily_returns)

    if aligned_portfolio is not None:
        variance = float(aligned_benchmark.var(ddof=1))
        if math.isfinite(variance) and variance != 0:
            covariance = float(aligned_portfolio.cov(aligned_benchmark))
            if math.isfinite(covariance):
                beta_value = covariance / variance
                if math.isfinite(beta_value):
                    beta = round(float(beta_value), 4)

                    portfolio_mean = float(aligned_portfolio.mean())
                    benchmark_mean = float(aligned_benchmark.mean())
                    trading_days = TRADING_DAYS_PER_YEAR
                    risk_free_daily = RISK_FREE_RATE_ANNUAL / trading_days
                    expected_return = risk_free_daily + beta_value * (benchmark_mean - risk_free_daily)
                    alpha_daily = portfolio_mean - expected_return
                    alpha_annualized_percent = round(alpha_daily * trading_days * 100, 2)
                    observations = len(aligned_portfolio)

    return {
        "sharpe_ratio": sharpe_ratio,
        "beta": beta,
        "alpha_annualized_percent": alpha_annualized_percent,
        "observations": observations,
    }


def _apply_imported_buy(
    db: Session,
    holding: ImportedHolding,
    quantity: float,
    price: float,
    txn_date: date,
    record_label: str = "BUY",
) -> None:
    existing_qty = _safe_number(holding.quantity)
    existing_invested = _safe_number(holding.invested_amount)
    new_qty = existing_qty + quantity
    new_invested = existing_invested + (quantity * price)
    holding.quantity = new_qty
    holding.invested_amount = round(new_invested, 2)
    holding.avg_buy_cost = round(new_invested / new_qty, 2)
    _refresh_single_imported_holding(holding)
    holding.imported_at = datetime.utcnow()

    db.add(
        ImportedHoldingTransaction(
            symbol=_normalize_symbol(holding.symbol),
            quantity=quantity,
            price=price,
            type=record_label,
            date=txn_date,
        )
    )


def process_due_sips(db: Session) -> Dict[str, int]:
    today = date.today()
    sips = db.query(RecurringSip).filter(
        RecurringSip.active == 1,
        RecurringSip.next_run_date <= today,
    ).order_by(RecurringSip.next_run_date.asc(), RecurringSip.id.asc()).all()

    processed = 0

    for sip in sips:
        holding = db.query(ImportedHolding).filter(
            func.upper(ImportedHolding.symbol) == _normalize_symbol(sip.symbol)
        ).first()
        if not holding:
            continue

        run_date = sip.next_run_date
        while run_date <= today:
            _refresh_single_imported_holding(holding)
            unit_price = _first_finite(
                _fetch_close_for_date(holding, run_date),
                holding.current_price,
                holding.avg_buy_cost,
            )
            if not unit_price or unit_price <= 0:
                break

            quantity = sip.amount / unit_price
            _apply_imported_buy(
                db,
                holding,
                quantity=quantity,
                price=unit_price,
                txn_date=run_date,
                record_label="SIP_BUY",
            )
            processed += 1
            target_month_date = _add_months(run_date.replace(day=1), 1)
            month_lengths = [31, 29 if target_month_date.year % 4 == 0 and (target_month_date.year % 100 != 0 or target_month_date.year % 400 == 0) else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
            sip.next_run_date = date(
                target_month_date.year,
                target_month_date.month,
                min(sip.day_of_month, month_lengths[target_month_date.month - 1]),
            )
            try:
                db.commit()
            except Exception:
                db.rollback()
                break
            _upsert_imported_portfolio_snapshot(
                db,
                snapshot_date=run_date,
                allow_empty=True,
                overwrite=True,
            )
            run_date = sip.next_run_date

    return {"processed_sips": processed}


def create_recurring_sip(db: Session, payload: RecurringSipCreate):
    holding = db.query(ImportedHolding).filter(
        func.upper(ImportedHolding.symbol) == _normalize_symbol(payload.symbol)
    ).first()
    if not holding:
        raise ValueError(f"No imported mutual fund found for {payload.symbol}")

    if _normalize_asset_type(holding.asset_type) != "MUTUAL_FUND":
        raise ValueError("Recurring SIP is currently supported only for mutual funds")

    sip = RecurringSip(
        symbol=_normalize_symbol(payload.symbol),
        amount=_safe_number(payload.amount),
        start_date=payload.start_date,
        next_run_date=payload.start_date,
        day_of_month=payload.start_date.day,
        active=1,
    )
    db.add(sip)
    db.commit()
    db.refresh(sip)
    process_due_sips(db)

    return {
        "id": sip.id,
        "symbol": sip.symbol,
        "amount": round(sip.amount, 2),
        "start_date": sip.start_date.isoformat(),
        "next_run_date": sip.next_run_date.isoformat(),
        "day_of_month": sip.day_of_month,
        "active": bool(sip.active),
    }


def _build_normalized_performance_comparison(
    db: Session,
    benchmark_symbol: str = "^NSEI",
    snapshot_model=PortfolioSnapshot,
    performance_period: str = "1Y",
) -> Dict[str, object]:
    period_key = (performance_period or "1Y").strip().upper()
    lookback_years = {"1Y": 1, "3Y": 3, "5Y": 5}.get(period_key, 1)

    snapshots = db.query(snapshot_model).order_by(snapshot_model.date.asc()).all()

    if len(snapshots) < 2:
        return {
            "benchmark": benchmark_symbol,
            "points": [],
            "start_date": None,
            "end_date": None,
            "observations": 0,
        }

    snapshot_dates = [
        snapshot.date
        for snapshot in snapshots
        if snapshot.date is not None
    ]
    if len(snapshot_dates) < 2:
        return {
            "benchmark": benchmark_symbol,
            "points": [],
            "start_date": None,
            "end_date": None,
            "observations": 0,
        }

    latest_snapshot_date = max(snapshot_dates)
    lookback_start = latest_snapshot_date - timedelta(days=lookback_years * 365)

    snapshot_rows = [
        {"date": snapshot.date, "portfolio_value": _safe_number(snapshot.total_value)}
        for snapshot in snapshots
        if (
            snapshot.date
            and snapshot.date >= lookback_start
            and snapshot.total_value is not None
            and _safe_number(snapshot.total_value) >= 0
        )
    ]
    if len(snapshot_rows) < 2:
        return {
            "benchmark": benchmark_symbol,
            "points": [],
            "start_date": None,
            "end_date": None,
            "observations": 0,
        }

    start_date = snapshot_rows[0]["date"]
    end_date = snapshot_rows[-1]["date"] + timedelta(days=1)

    try:
        benchmark_data = yf.download(
            benchmark_symbol,
            start=start_date,
            end=end_date,
            auto_adjust=True,
            progress=False,
        )
    except Exception:
        return {
            "benchmark": benchmark_symbol,
            "points": [],
            "start_date": None,
            "end_date": None,
            "observations": 0,
        }

    if benchmark_data.empty or "Close" not in benchmark_data:
        return {
            "benchmark": benchmark_symbol,
            "points": [],
            "start_date": None,
            "end_date": None,
            "observations": 0,
        }

    benchmark_close = benchmark_data["Close"]
    if isinstance(benchmark_close, pd.DataFrame):
        if benchmark_close.empty or benchmark_close.shape[1] == 0:
            return {
                "benchmark": benchmark_symbol,
                "points": [],
                "start_date": None,
                "end_date": None,
                "observations": 0,
            }
        benchmark_close = benchmark_close.iloc[:, 0]

    benchmark_close = pd.to_numeric(benchmark_close, errors="coerce").dropna()
    if benchmark_close.empty:
        return {
            "benchmark": benchmark_symbol,
            "points": [],
            "start_date": None,
            "end_date": None,
            "observations": 0,
        }

    benchmark_close.index = pd.to_datetime(benchmark_close.index).date
    benchmark_by_date = {
        idx: float(value)
        for idx, value in benchmark_close.items()
        if math.isfinite(float(value)) and float(value) > 0
    }

    common_dates = sorted(
        row["date"]
        for row in snapshot_rows
        if row["date"] in benchmark_by_date
    )
    if len(common_dates) < 2:
        return {
            "benchmark": benchmark_symbol,
            "points": [],
            "start_date": None,
            "end_date": None,
            "observations": 0,
        }

    portfolio_start = next(
        row["portfolio_value"] for row in snapshot_rows if row["date"] == common_dates[0]
    )
    benchmark_start = benchmark_by_date[common_dates[0]]
    if portfolio_start <= 0 or benchmark_start <= 0:
        return {
            "benchmark": benchmark_symbol,
            "points": [],
            "start_date": None,
            "end_date": None,
            "observations": 0,
        }

    portfolio_by_date = {
        row["date"]: row["portfolio_value"]
        for row in snapshot_rows
    }
    points = []
    for point_date in common_dates:
        portfolio_value = (portfolio_by_date[point_date] / portfolio_start) * 100
        benchmark_value = (benchmark_by_date[point_date] / benchmark_start) * 100
        points.append(
            {
                "date": point_date.isoformat(),
                "portfolio_value": round(portfolio_value, 2),
                "benchmark_value": round(benchmark_value, 2),
                "portfolio_change_percent": round(portfolio_value - 100, 2),
                "benchmark_change_percent": round(benchmark_value - 100, 2),
            }
        )

    return {
        "benchmark": benchmark_symbol,
        "points": points,
        "start_date": common_dates[0].isoformat(),
        "end_date": common_dates[-1].isoformat(),
        "observations": len(points),
    }


def get_imported_portfolio_dashboard(
    db: Session,
    category: str = "ALL",
    performance_period: str = "1Y",
):
    _ensure_imported_snapshot_history(db)
    process_due_sips(db)
    normalized_category = category.strip().upper() if category else "ALL"
    holdings = db.query(ImportedHolding).order_by(ImportedHolding.symbol.asc()).all()

    available_categories = ["ALL"] + sorted(
        {_display_asset_type(_normalize_asset_type(holding.asset_type)) for holding in holdings}
    )

    if normalized_category != "ALL":
        holdings = [
            holding
            for holding in holdings
            if _display_asset_type(_normalize_asset_type(holding.asset_type)).upper().replace(" ", "_")
            == normalized_category.replace(" ", "_")
        ]

    rows = [_serialize_imported_holding(holding) for holding in holdings]
    total_current_value = sum(_safe_number(row.get("current_value")) for row in rows)

    for row in rows:
        current_value = _safe_number(row.get("current_value"))
        row["weight_percent"] = round((current_value / total_current_value) * 100, 2) if total_current_value else 0.0

    total_invested = sum(_safe_number(row.get("invested_amount")) for row in rows)
    total_gain = sum(_safe_number(row.get("unrealized_pnl")) for row in rows)
    one_day_change = sum(_safe_number(row.get("one_day_change")) for row in rows)
    previous_day_value = total_current_value - one_day_change
    one_day_change_percent = (
        (one_day_change / previous_day_value) * 100
        if previous_day_value > 0
        else 0.0
    )
    total_gain_percent = ((total_gain / total_invested) * 100) if total_invested else 0.0

    weighted_pe_numerator = sum(
        _safe_number(row.get("current_value")) * _safe_number(row.get("pe_ratio"))
        for row in rows
        if row.get("pe_ratio") is not None
    )
    weighted_pe_denominator = sum(
        _safe_number(row.get("current_value"))
        for row in rows
        if row.get("pe_ratio") is not None
    )
    portfolio_avg_pe = (
        round(weighted_pe_numerator / weighted_pe_denominator, 2)
        if weighted_pe_denominator
        else None
    )

    latest_import = db.query(ImportedHolding).order_by(ImportedHolding.imported_at.desc()).first()
    nifty_chart = _fetch_benchmark_mini_chart("^NSEI", "Nifty 50")
    sensex_chart = _fetch_benchmark_mini_chart("^BSESN", "Sensex")

    benchmark = _fetch_benchmark_summary("^NSEI")
    if nifty_chart.get("current_level") is not None:
        benchmark["price"] = nifty_chart.get("current_level")
    if nifty_chart.get("prev_close") is not None:
        benchmark["prev_close"] = nifty_chart.get("prev_close")
    if nifty_chart.get("change_percent") is not None:
        benchmark["one_day_change_percent"] = nifty_chart.get("change_percent")
    if nifty_chart.get("name"):
        benchmark["name"] = nifty_chart.get("name")

    benchmark_pe = benchmark.get("pe_ratio")
    risk_metrics = _calculate_imported_risk_metrics(db, "^NSEI")
    recurring_sips = db.query(RecurringSip).order_by(RecurringSip.next_run_date.asc(), RecurringSip.id.asc()).all()

    return {
        "overview": {
            "total_net_worth": round(total_current_value, 2),
            "total_gain": round(total_gain, 2),
            "total_gain_percent": round(total_gain_percent, 2),
            "one_day_change": round(one_day_change, 2),
            "one_day_change_percent": round(one_day_change_percent, 2),
            "holdings_count": len(rows),
            "as_of": latest_import.imported_at.isoformat() if latest_import else None,
            "selected_category": "All" if normalized_category == "ALL" else normalized_category.replace("_", " ").title(),
            "available_categories": available_categories,
        },
        "holdings": rows,
        "asset_allocation": _bucketize(rows, "asset_type"),
        "sector_allocation": _bucketize(rows, "sector"),
        "benchmark": benchmark,
        "benchmark_charts": [
            nifty_chart,
            sensex_chart,
        ],
        "risk_metrics": risk_metrics,
        "performance_comparison": _build_normalized_performance_comparison(
            db,
            "^NSEI",
            snapshot_model=ImportedPortfolioSnapshot,
            performance_period=performance_period,
        ),
        "recurring_sips": [
            {
                "id": sip.id,
                "symbol": sip.symbol,
                "amount": round(_safe_number(sip.amount), 2),
                "start_date": sip.start_date.isoformat(),
                "next_run_date": sip.next_run_date.isoformat(),
                "day_of_month": int(sip.day_of_month),
                "active": bool(sip.active),
            }
            for sip in recurring_sips
        ],
        "portfolio_avg_pe": portfolio_avg_pe,
        "benchmark_pe_gap": round(portfolio_avg_pe - benchmark_pe, 2)
        if portfolio_avg_pe is not None and benchmark_pe is not None
        else None,
        "import_file_name": latest_import.source_file if latest_import else None,
        "imported_at": latest_import.imported_at.isoformat() if latest_import else None,
    }
