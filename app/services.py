from datetime import date, timedelta
import math
from typing import Dict, List, Optional

import pandas as pd
import yfinance as yf
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.models import Holding, PortfolioSnapshot, Price, Transaction
from app.schemas import TransactionCreate

EPSILON = 1e-9


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
    db: Session, snapshot_date: date, portfolio_data: Dict[str, float]
) -> bool:
    existing_snapshot = db.query(PortfolioSnapshot).filter(
        PortfolioSnapshot.date == snapshot_date
    ).first()

    if existing_snapshot:
        return False

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
        total_invested += invested

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

    risk_free_rate_daily = 0.0
    sharpe_ratio = (mean_return - risk_free_rate_daily) / volatility

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

def calculate_alpha(db: Session, benchmark_symbol: str = "^NSEI", risk_free_rate_annual: float = 0.05):
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

    trading_days = 252
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

    trading_days = 252
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

    trading_days = 252
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
