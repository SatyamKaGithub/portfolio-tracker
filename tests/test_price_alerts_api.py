def _auth_header(client, username: str, email: str, password: str = "strongpass123"):
    signup_response = client.post(
        "/auth/signup",
        json={
            "username": username,
            "email": email,
            "password": password,
        },
    )
    assert signup_response.status_code == 200

    login_response = client.post(
        "/auth/login",
        json={"login": email, "password": password},
    )
    assert login_response.status_code == 200
    token = login_response.json()["token"]
    return {"Authorization": f"Bearer {token}"}


def test_create_and_list_price_alerts(client):
    headers = _auth_header(client, "alerts_user", "alerts@example.com")

    create_response = client.post(
        "/alerts/price",
        headers=headers,
        json={
            "symbol": "INFY",
            "target_price": 1700,
            "direction": "ABOVE",
            "duration": "1_MONTH",
            "channel": "IN_APP",
        },
    )
    assert create_response.status_code == 200
    created = create_response.json()
    assert created["symbol"] == "INFY"
    assert created["status"] == "ACTIVE"

    list_response = client.get("/alerts/price", headers=headers)
    assert list_response.status_code == 200
    rows = list_response.json()
    assert len(rows) == 1
    assert rows[0]["symbol"] == "INFY"


def test_price_alert_check_triggers_notification(client, monkeypatch):
    headers = _auth_header(client, "alerts_user_2", "alerts2@example.com")

    create_response = client.post(
        "/alerts/price",
        headers=headers,
        json={
            "symbol": "RELIANCE",
            "target_price": 2500,
            "direction": "ABOVE",
            "duration": "UNTIL_HIT",
            "channel": "IN_APP",
        },
    )
    assert create_response.status_code == 200

    monkeypatch.setattr("app.services._resolve_alert_market_price", lambda symbol: 2600.0)

    run_response = client.post("/alerts/price/check", headers=headers)
    assert run_response.status_code == 200
    run_body = run_response.json()
    assert run_body["checked_alerts"] == 1
    assert run_body["triggered_alerts"] == 1

    alerts_response = client.get("/alerts/price", headers=headers)
    alert_rows = alerts_response.json()
    assert alert_rows[0]["status"] == "TRIGGERED"
    assert alert_rows[0]["triggered_price"] == 2600.0

    inbox_response = client.get("/notifications", headers=headers)
    assert inbox_response.status_code == 200
    notifications = inbox_response.json()
    assert len(notifications) == 1
    assert notifications[0]["channel"] == "IN_APP"
    assert notifications[0]["read"] is False


def test_mark_notification_read(client, monkeypatch):
    headers = _auth_header(client, "alerts_user_3", "alerts3@example.com")

    client.post(
        "/alerts/price",
        headers=headers,
        json={
            "symbol": "TCS",
            "target_price": 3900,
            "direction": "BELOW",
            "duration": "1_WEEK",
            "channel": "IN_APP",
        },
    )

    monkeypatch.setattr("app.services._resolve_alert_market_price", lambda symbol: 3800.0)
    client.post("/alerts/price/check", headers=headers)

    inbox_response = client.get("/notifications", headers=headers)
    notification = inbox_response.json()[0]

    read_response = client.post(
        f"/notifications/{notification['id']}/read",
        headers=headers,
    )
    assert read_response.status_code == 200
    assert read_response.json()["read"] is True


def test_price_alert_check_is_user_scoped(client, monkeypatch):
    headers_one = _auth_header(client, "alerts_user_4", "alerts4@example.com")
    headers_two = _auth_header(client, "alerts_user_5", "alerts5@example.com")

    client.post(
        "/alerts/price",
        headers=headers_one,
        json={
            "symbol": "SBIN",
            "target_price": 900,
            "direction": "ABOVE",
            "duration": "UNTIL_HIT",
            "channel": "IN_APP",
        },
    )
    client.post(
        "/alerts/price",
        headers=headers_two,
        json={
            "symbol": "INFY",
            "target_price": 1800,
            "direction": "ABOVE",
            "duration": "UNTIL_HIT",
            "channel": "IN_APP",
        },
    )

    monkeypatch.setattr("app.services._resolve_alert_market_price", lambda symbol: 5000.0)

    run_response = client.post("/alerts/price/check", headers=headers_one)
    assert run_response.status_code == 200
    assert run_response.json()["checked_alerts"] == 1

    user_one_notifications = client.get("/notifications", headers=headers_one).json()
    user_two_notifications = client.get("/notifications", headers=headers_two).json()
    assert len(user_one_notifications) == 1
    assert len(user_two_notifications) == 0
