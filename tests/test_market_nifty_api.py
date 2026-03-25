def test_market_nifty50_endpoint_returns_rows(client, monkeypatch):
    monkeypatch.setattr(
        "app.services._load_nifty50_ticker_rows",
        lambda: [
            {"symbol": "RELIANCE", "name": "Reliance", "price": 3000.0, "change_percent": 1.25},
            {"symbol": "TCS", "name": "TCS", "price": 4200.0, "change_percent": -0.75},
        ],
    )
    monkeypatch.setattr("app.services._NIFTY50_CACHE", {"loaded_at": 0.0, "rows": []})

    response = client.get("/market/nifty50")
    assert response.status_code == 200

    body = response.json()
    assert "rows" in body
    assert len(body["rows"]) == 2
    assert body["rows"][0]["symbol"] == "RELIANCE"
