from sqlalchemy.orm import Session
from app.models import Holding, Price, PortfolioSnapshot
from datetime import date
import yfinance as yf


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