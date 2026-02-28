from sqlalchemy.orm import Session
from app.models import Holding, Price, PortfolioSnapshot
from datetime import date
import yfinance as yf
import math


def update_prices(db: Session):
    holdings = db.query(Holding).all()
    today = date.today()

    for h in holdings:
        ticker = yf.Ticker(h.symbol)
        current_price = float(ticker.history(period="1d")["Close"].iloc[-1])

        existing_price = db.query(Price).filter(
            Price.symbol == h.symbol,
            Price.date == today
        ).first()

        if not existing_price:
            new_price = Price(
                symbol=h.symbol,
                price=current_price,
                date=today
            )
            db.add(new_price)

    db.commit()

    # After prices are updated, calculate and store portfolio snapshot
    portfolio_data = calculate_portfolio_value(db)

    existing_snapshot = db.query(PortfolioSnapshot).filter(
        PortfolioSnapshot.date == today
    ).first()

    if not existing_snapshot:
        snapshot = PortfolioSnapshot(
            total_value=portfolio_data["total_current_value"],
            total_invested=portfolio_data["total_invested"],
            pnl=portfolio_data["total_pnl"],
            date=today
        )
        db.add(snapshot)
        db.commit()


def calculate_portfolio_value(db: Session):
    holdings = db.query(Holding).all()
    today = date.today()

    total_invested = 0
    total_current_value = 0

    for h in holdings:
        price_record = db.query(Price).filter(
            Price.symbol == h.symbol,
            Price.date == today
        ).first()

        if not price_record:
            continue

        invested = h.quantity * h.avg_price
        current_value = h.quantity * price_record.price

        total_invested += invested
        total_current_value += current_value

    total_pnl = total_current_value - total_invested

    return {
        "total_invested": round(total_invested, 2),
        "total_current_value": round(total_current_value, 2),
        "total_pnl": round(total_pnl, 2)
    }

def calculate_performance_metrics(db: Session):
    snapshots = db.query(PortfolioSnapshot).order_by(PortfolioSnapshot.date).all()

    if len(snapshots) < 2:
        return {"message": "Not enough data for performance calculation"}

    start_value = snapshots[0].total_value
    latest_value = snapshots[-1].total_value

    absolute_return_percent = ((latest_value - start_value) / start_value) * 100

    return {
        "start_value": start_value,
        "latest_value": latest_value,
        "absolute_return_percent": round(absolute_return_percent, 2)
    }

def calculate_max_drawdown(db: Session):
    snapshots = db.query(PortfolioSnapshot).order_by(PortfolioSnapshot.date).all()

    if len(snapshots) < 2:
        return {"message": "Not enough data for drawdown calculation"}

    peak = snapshots[0].total_value
    max_drawdown = 0

    for snapshot in snapshots:
        value = snapshot.total_value

        if value > peak:
            peak = value

        drawdown = (value - peak) / peak

        if drawdown < max_drawdown:
            max_drawdown = drawdown

    return {
        "max_drawdown_percent": round(max_drawdown * 100, 2)
    }

def calculate_volatility(db: Session):
    snapshots = db.query(PortfolioSnapshot).order_by(PortfolioSnapshot.date).all()

    if len(snapshots) < 2:
        return {"message": "Not enough data for volatility calculation"}

    daily_returns = []

    for i in range(1, len(snapshots)):
        prev = snapshots[i - 1].total_value
        current = snapshots[i].total_value

        daily_return = (current - prev) / prev
        daily_returns.append(daily_return)

    mean_return = sum(daily_returns) / len(daily_returns)

    variance = sum((r - mean_return) ** 2 for r in daily_returns) / len(daily_returns)

    volatility = math.sqrt(variance)

    return {
        "volatility_percent": round(volatility * 100, 2)
    }

def calculate_sharpe_ratio(db: Session):
    snapshots = db.query(PortfolioSnapshot).order_by(PortfolioSnapshot.date).all()

    if len(snapshots) < 2:
        return {"message": "Not enough data for Sharpe ratio calculation"}

    daily_returns = []

    for i in range(1, len(snapshots)):
        prev = snapshots[i - 1].total_value
        current = snapshots[i].total_value
        daily_return = (current - prev) / prev
        daily_returns.append(daily_return)

    mean_return = sum(daily_returns) / len(daily_returns)

    variance = sum((r - mean_return) ** 2 for r in daily_returns) / len(daily_returns)
    volatility = math.sqrt(variance)

    if volatility == 0:
        return {"message": "Volatility is zero, Sharpe ratio undefined"}

    risk_free_rate = 0  # simplifying assumption

    sharpe_ratio = (mean_return - risk_free_rate) / volatility

    return {
        "sharpe_ratio": round(sharpe_ratio, 4)
    }

def calculate_rolling_volatility(db: Session, window: int = 3):
    snapshots = db.query(PortfolioSnapshot).order_by(PortfolioSnapshot.date).all()

    if len(snapshots) <= window:
        return {"message": "Not enough data for rolling calculation"}

    daily_returns = []

    for i in range(1, len(snapshots)):
        prev = snapshots[i - 1].total_value
        current = snapshots[i].total_value
        daily_return = (current - prev) / prev
        daily_returns.append(daily_return)

    rolling_vol = []

    for i in range(window, len(daily_returns) + 1):
        window_slice = daily_returns[i - window:i]
        mean_return = sum(window_slice) / window
        variance = sum((r - mean_return) ** 2 for r in window_slice) / window
        vol = math.sqrt(variance)
        rolling_vol.append(round(vol * 100, 2))

    return {
        "window": window,
        "rolling_volatility_percent": rolling_vol
    }

def calculate_beta(db: Session, benchmark_symbol: str = "^NSEI"):
    snapshots = db.query(PortfolioSnapshot).order_by(PortfolioSnapshot.date).all()

    if len(snapshots) < 2:
        return {"message": "Not enough data for beta calculation"}

    # Portfolio daily returns
    portfolio_returns = []
    for i in range(1, len(snapshots)):
        prev = snapshots[i - 1].total_value
        current = snapshots[i].total_value
        portfolio_returns.append((current - prev) / prev)

    # Fetch benchmark historical data
    import yfinance as yf
    import pandas as pd

    start_date = snapshots[0].date
    end_date = snapshots[-1].date

    benchmark_data = yf.download(benchmark_symbol, start=start_date, end=end_date)

    if len(benchmark_data) < 2:
        return {"message": "Not enough benchmark data"}

    benchmark_returns = benchmark_data["Close"].pct_change().dropna().tolist()

    # Align lengths (simplified assumption)
    min_length = min(len(portfolio_returns), len(benchmark_returns))
    portfolio_returns = portfolio_returns[-min_length:]
    benchmark_returns = benchmark_returns[-min_length:]

    # Compute covariance and variance
    import numpy as np

    covariance = np.cov(portfolio_returns, benchmark_returns)[0][1]
    variance = np.var(benchmark_returns)

    if variance == 0:
        return {"message": "Benchmark variance is zero"}

    beta = covariance / variance

    return {
        "benchmark": benchmark_symbol,
        "beta": round(beta, 4)
    }