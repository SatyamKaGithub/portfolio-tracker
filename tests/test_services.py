from datetime import date, timedelta

from app.models import Price, Transaction
from app.services import calculate_holdings_from_transactions, portfolio_value_from_ledger


def test_calculate_holdings_from_transactions_tracks_negative_symbols(db_session):
    today = date.today()
    db_session.add_all(
        [
            Transaction(symbol="INFY", quantity=2, price=100, type="BUY", date=today),
            Transaction(symbol="INFY", quantity=3, price=120, type="SELL", date=today),
        ]
    )
    db_session.commit()

    result = calculate_holdings_from_transactions(db_session)

    assert result["holdings"] == {}
    assert result["negative_symbols"] == ["INFY"]


def test_portfolio_value_from_ledger_uses_latest_price_before_as_of(db_session):
    base_date = date(2026, 1, 10)

    db_session.add_all(
        [
            Transaction(symbol="RELIANCE", quantity=4, price=200, type="BUY", date=base_date),
            Price(symbol="RELIANCE", price=250, date=base_date),
            Price(symbol="RELIANCE", price=275, date=base_date + timedelta(days=2)),
        ]
    )
    db_session.commit()

    result_base = portfolio_value_from_ledger(db_session, as_of=base_date)
    assert result_base["total_value"] == 1000
    assert result_base["missing_price_symbols"] == []
    assert result_base["negative_symbols"] == []

    result_later = portfolio_value_from_ledger(db_session, as_of=base_date + timedelta(days=3))
    assert result_later["total_value"] == 1100
