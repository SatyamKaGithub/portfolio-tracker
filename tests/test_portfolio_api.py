from datetime import date, timedelta

from app.models import Holding, PortfolioSnapshot, Price


def _insert_snapshots(db_session, values):
    start_date = date(2026, 1, 1)
    for index, value in enumerate(values):
        db_session.add(
            PortfolioSnapshot(
                total_value=value,
                total_invested=100,
                pnl=value - 100,
                date=start_date + timedelta(days=index),
            )
        )
    db_session.commit()


def test_portfolio_value_includes_missing_and_stale_symbols(client, db_session):
    today = date.today()
    yesterday = today - timedelta(days=1)

    db_session.add_all(
        [
            Holding(symbol="INFY", quantity=2, avg_price=100),
            Holding(symbol="TCS", quantity=1, avg_price=200),
            Holding(symbol="SBIN", quantity=5, avg_price=50),
            Price(symbol="INFY", price=150, date=today),
            Price(symbol="TCS", price=300, date=yesterday),
        ]
    )
    db_session.commit()

    response = client.get("/portfolio/value")
    assert response.status_code == 200
    body = response.json()

    assert body["total_invested"] == 400
    assert body["total_current_value"] == 600
    assert body["total_pnl"] == 200
    assert body["missing_price_symbols"] == ["SBIN"]
    assert body["stale_price_symbols"] == ["TCS"]


def test_performance_and_drawdown_metrics(client, db_session):
    _insert_snapshots(db_session, [100, 120, 90, 125])

    perf_response = client.get("/portfolio/performance")
    assert perf_response.status_code == 200
    perf_body = perf_response.json()
    assert perf_body["start_value"] == 100
    assert perf_body["latest_value"] == 125
    assert perf_body["absolute_return_percent"] == 25

    drawdown_response = client.get("/portfolio/drawdown")
    assert drawdown_response.status_code == 200
    drawdown_body = drawdown_response.json()
    assert drawdown_body["max_drawdown_percent"] == -25


def test_daily_returns_limit_and_rolling_volatility(client, db_session):
    _insert_snapshots(db_session, [100, 110, 121, 133.1])

    daily_response = client.get("/portfolio/daily-returns", params={"limit": 2})
    assert daily_response.status_code == 200
    daily_body = daily_response.json()

    assert daily_body["observations"] == 2
    assert len(daily_body["daily_returns"]) == 2
    assert daily_body["daily_returns"][0]["daily_return_percent"] == 10
    assert daily_body["daily_returns"][1]["daily_return_percent"] == 10

    rolling_response = client.get("/portfolio/rolling-volatility", params={"window": 3})
    assert rolling_response.status_code == 200
    rolling_body = rolling_response.json()

    assert rolling_body["window"] == 3
    assert rolling_body["rolling_volatility_percent"] == [0.0]
    assert rolling_body["observations"] == 3
