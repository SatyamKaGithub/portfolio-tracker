from datetime import date, timedelta


def test_create_buy_transaction_creates_holding(client, auth_headers):
    payload = {
        "symbol": "  infy ",
        "quantity": 10,
        "price": 100,
        "type": "buy",
    }

    response = client.post("/transactions", json=payload, headers=auth_headers)
    assert response.status_code == 200

    body = response.json()
    assert body["symbol"] == "INFY"
    assert body["type"] == "BUY"

    holdings_response = client.get("/holdings", headers=auth_headers)
    assert holdings_response.status_code == 200
    holdings = holdings_response.json()

    assert len(holdings) == 1
    assert holdings[0]["symbol"] == "INFY"
    assert holdings[0]["quantity"] == 10
    assert holdings[0]["avg_price"] == 100


def test_sell_more_than_available_returns_400(client, auth_headers):
    client.post(
        "/transactions",
        json={"symbol": "TCS", "quantity": 5, "price": 200, "type": "BUY"},
        headers=auth_headers,
    )

    response = client.post(
        "/transactions",
        json={"symbol": "TCS", "quantity": 6, "price": 220, "type": "SELL"},
        headers=auth_headers,
    )

    assert response.status_code == 400
    assert "exceeds available holdings" in response.json()["detail"]


def test_batch_transactions_apply_weighted_average_and_sell(client, auth_headers):
    payload = [
        {"symbol": "HDFCBANK", "quantity": 10, "price": 100, "type": "BUY"},
        {"symbol": "HDFCBANK", "quantity": 5, "price": 160, "type": "BUY"},
        {"symbol": "HDFCBANK", "quantity": 3, "price": 150, "type": "SELL"},
    ]

    response = client.post("/transactions", json=payload, headers=auth_headers)
    assert response.status_code == 200
    assert len(response.json()) == 3

    holdings = client.get("/holdings", headers=auth_headers).json()
    assert len(holdings) == 1
    assert holdings[0]["symbol"] == "HDFCBANK"
    assert holdings[0]["quantity"] == 12
    assert holdings[0]["avg_price"] == 120


def test_future_transaction_date_fails_validation(client, auth_headers):
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
        headers=auth_headers,
    )

    assert response.status_code == 422


def test_transactions_are_isolated_per_user(client, auth_headers):
    client.post(
        "/transactions",
        json={"symbol": "INFY", "quantity": 1, "price": 100, "type": "BUY"},
        headers=auth_headers,
    )

    client.post(
        "/auth/signup",
        json={
            "username": "otheruser",
            "email": "other@example.com",
            "password": "strongpass123",
        },
    )
    login_response = client.post(
        "/auth/login",
        json={"login": "other@example.com", "password": "strongpass123"},
    )
    other_headers = {"Authorization": f"Bearer {login_response.json()['token']}"}

    other_transactions = client.get("/transactions", headers=other_headers)
    assert other_transactions.status_code == 200
    assert other_transactions.json() == []
