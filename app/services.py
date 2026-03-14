from datetime import date, datetime, timedelta
import math
from typing import Dict, List, Optional

import pandas as pd
import yfinance as yf
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.importers import decode_base64_document, parse_xlsx_holdings
from app.models import Holding, ImportedHolding, PortfolioSnapshot, Price, Transaction
from app.schemas import HoldingsImportPayload, TransactionCreate

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

        holding.exchange_symbol = str(snapshot.get("exchange_symbol") or holding.exchange_symbol or "")
        holding.company_name = str(snapshot.get("company_name") or holding.company_name or "")
        holding.sector = str(snapshot.get("sector") or holding.sector or "")
        holding.geography = str(snapshot.get("geography") or holding.geography or "India")
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

    return {
        "message": "Imported holdings refreshed",
        "updated_count": updated,
        "failed_symbols": failed_symbols,
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
    price = float(closes.iloc[-1]) if len(closes) else None
    prev_close = float(closes.iloc[-2]) if len(closes) > 1 else price
    change_percent = None
    if price is not None and prev_close not in (None, 0):
        change_percent = ((price - prev_close) / prev_close) * 100

    try:
        info = ticker.info or {}
    except Exception:
        info = {}

    pe_ratio = _first_finite(info.get("trailingPE"), info.get("forwardPE")) if info else None

    return {
        "symbol": symbol,
        "name": str(info.get("shortName") or "Nifty 50") if info else "Nifty 50",
        "price": round(price, 2) if price is not None else None,
        "prev_close": round(prev_close, 2) if prev_close is not None else None,
        "one_day_change_percent": round(change_percent, 2) if change_percent is not None else None,
        "pe_ratio": round(pe_ratio, 2) if pe_ratio is not None else None,
    }


def get_imported_portfolio_dashboard(db: Session, category: str = "ALL"):
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
        if previous_day_value not in (0, None)
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
    benchmark = _fetch_benchmark_summary("^NSEI")
    benchmark_pe = benchmark.get("pe_ratio")

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
        "portfolio_avg_pe": portfolio_avg_pe,
        "benchmark_pe_gap": round(portfolio_avg_pe - benchmark_pe, 2)
        if portfolio_avg_pe is not None and benchmark_pe is not None
        else None,
        "import_file_name": latest_import.source_file if latest_import else None,
        "imported_at": latest_import.imported_at.isoformat() if latest_import else None,
    }
