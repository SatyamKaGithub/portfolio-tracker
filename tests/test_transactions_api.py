from datetime import date, timedelta


def test_create_buy_transaction_creates_holding(client):
    payload = {
        "symbol": "  infy ",
        "quantity": 10,
        "price": 100,
        "type": "buy",
    }

    response = client.post("/transactions", json=payload)
    assert response.status_code == 200

    body = response.json()
    assert body["symbol"] == "INFY"
    assert body["type"] == "BUY"

    holdings_response = client.get("/holdings")
    assert holdings_response.status_code == 200
    holdings = holdings_response.json()

    assert len(holdings) == 1
    assert holdings[0]["symbol"] == "INFY"
    assert holdings[0]["quantity"] == 10
    assert holdings[0]["avg_price"] == 100


def test_sell_more_than_available_returns_400(client):
    client.post(
        "/transactions",
        json={"symbol": "TCS", "quantity": 5, "price": 200, "type": "BUY"},
    )

    response = client.post(
        "/transactions",
        json={"symbol": "TCS", "quantity": 6, "price": 220, "type": "SELL"},
    )

    assert response.status_code == 400
    assert "exceeds available holdings" in response.json()["detail"]


def test_batch_transactions_apply_weighted_average_and_sell(client):
    payload = [
        {"symbol": "HDFCBANK", "quantity": 10, "price": 100, "type": "BUY"},
        {"symbol": "HDFCBANK", "quantity": 5, "price": 160, "type": "BUY"},
        {"symbol": "HDFCBANK", "quantity": 3, "price": 150, "type": "SELL"},
    ]

    response = client.post("/transactions", json=payload)
    assert response.status_code == 200
    assert len(response.json()) == 3

    holdings = client.get("/holdings").json()
    assert len(holdings) == 1
    assert holdings[0]["symbol"] == "HDFCBANK"
    assert holdings[0]["quantity"] == 12
    assert holdings[0]["avg_price"] == 120


def test_future_transaction_date_fails_validation(client):
    tomorrow = (date.today() + timedelta(days=1)).isoformat()
    response = client.post(
        "/transactions",
        json={
            "symbol": "SBIN",
            "quantity": 2,
            "price": 700,
            "type": "BUY",
            "date": tomorrow,
        },
    )

    assert response.status_code == 422
