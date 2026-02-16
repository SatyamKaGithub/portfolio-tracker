from sqlalchemy.orm import Session
from app.models import Holding, Price
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