from datetime import datetime

from app.models import ImportedHolding


def test_add_recurring_sip_endpoint_success(client, db_session, monkeypatch, auth_headers):
    db_session.add(
        ImportedHolding(
            user_id=1,
            symbol="AXISMF",
            company_name="Axis Flexicap",
            asset_type="MUTUAL_FUND",
            quantity=10,
            avg_buy_cost=100,
            invested_amount=1000,
            current_price=110,
            current_value=1100,
            one_day_change=0,
            unrealized_pnl=100,
            currency="INR",
            imported_at=datetime(2026, 1, 1, 9, 0, 0),
        )
    )
    db_session.commit()

    monkeypatch.setattr("app.services.process_due_sips", lambda *args, **kwargs: {"processed_sips": 0})

    response = client.post(
        "/imports/holdings/sips",
        json={"symbol": "AXISMF", "amount": 2000, "start_date": "2026-01-10"},
        headers=auth_headers,
    )
    assert response.status_code == 200
    body = response.json()

    assert body["symbol"] == "AXISMF"
    assert body["amount"] == 2000
    assert body["start_date"] == "2026-01-10"
    assert body["next_run_date"] == "2026-01-10"
    assert body["active"] is True


def test_add_recurring_sip_endpoint_returns_400_for_stock(client, db_session, auth_headers):
    db_session.add(
        ImportedHolding(
            user_id=1,
            symbol="INFY",
            company_name="Infosys",
            asset_type="STOCK",
            quantity=1,
            avg_buy_cost=1000,
            invested_amount=1000,
            current_price=1050,
            current_value=1050,
            one_day_change=0,
            unrealized_pnl=50,
            currency="INR",
            imported_at=datetime(2026, 1, 1, 9, 0, 0),
        )
    )
    db_session.commit()

    response = client.post(
        "/imports/holdings/sips",
        json={"symbol": "INFY", "amount": 500, "start_date": "2026-01-10"},
        headers=auth_headers,
    )

    assert response.status_code == 400
    assert "currently supported only for mutual funds" in response.json()["detail"]


def test_imported_transaction_endpoint_buy_updates_holding(client, db_session, monkeypatch, auth_headers):
    db_session.add(
        ImportedHolding(
            user_id=1,
            symbol="HDFCBANK",
            company_name="HDFC Bank",
            asset_type="STOCK",
            quantity=2,
            avg_buy_cost=1000,
            invested_amount=2000,
            current_price=1100,
            prev_close=1080,
            current_value=2200,
            one_day_change=40,
            unrealized_pnl=200,
            currency="INR",
            imported_at=datetime(2026, 1, 1, 9, 0, 0),
        )
    )
    db_session.commit()

    monkeypatch.setattr("app.services._refresh_single_imported_holding", lambda *_: None)
    monkeypatch.setattr("app.services._upsert_imported_portfolio_snapshot", lambda *args, **kwargs: True)

    response = client.post(
        "/imports/holdings/transactions",
        json={
            "symbol": "HDFCBANK",
            "quantity": 1,
            "price": 1200,
            "type": "BUY",
            "date": "2026-01-12",
        },
        headers=auth_headers,
    )
    assert response.status_code == 200

    body = response.json()
    assert body["symbol"] == "HDFCBANK"
    assert body["type"] == "BUY"
    assert body["date"] == "2026-01-12"


def test_imported_dashboard_endpoint_works_with_stubbed_dependencies(client, db_session, monkeypatch, auth_headers):
    db_session.add(
        ImportedHolding(
            user_id=1,
            symbol="AXISMF",
            company_name="Axis Flexicap",
            asset_type="MUTUAL_FUND",
            sector="Mutual Fund",
            quantity=5,
            avg_buy_cost=100,
            invested_amount=500,
            current_price=120,
            prev_close=118,
            current_value=600,
            one_day_change=10,
            unrealized_pnl=100,
            currency="INR",
            source_file="holdings.xlsx",
            imported_at=datetime(2026, 1, 6, 10, 0, 0),
        )
    )
    db_session.commit()

    monkeypatch.setattr("app.services._ensure_imported_snapshot_history", lambda *args, **kwargs: None)
    monkeypatch.setattr("app.services.process_due_sips", lambda *args, **kwargs: {"processed_sips": 0})
    monkeypatch.setattr(
        "app.services._fetch_benchmark_mini_chart",
        lambda symbol, name: {
            "symbol": symbol,
            "name": name,
            "current_level": 22500.0,
            "prev_close": 22400.0,
            "points_change": 100.0,
            "change_percent": 0.45,
            "trend": "positive",
            "points": [],
        },
    )
    monkeypatch.setattr(
        "app.services._fetch_benchmark_summary",
        lambda symbol="^NSEI": {
            "symbol": symbol,
            "name": "Nifty 50",
            "price": 0.0,
            "prev_close": 0.0,
            "one_day_change_percent": 0.0,
            "pe_ratio": 21.5,
        },
    )
    monkeypatch.setattr(
        "app.services._calculate_imported_risk_metrics",
        lambda *args, **kwargs: {
            "sharpe_ratio": 1.2,
            "beta": 0.8,
            "alpha_annualized_percent": 2.5,
            "observations": 10,
        },
    )
    monkeypatch.setattr(
        "app.services._build_normalized_performance_comparison",
        lambda *args, **kwargs: {
            "benchmark": "^NSEI",
            "points": [],
            "start_date": None,
            "end_date": None,
            "observations": 0,
        },
    )

    response = client.get(
        "/portfolio/imported-dashboard",
        params={"category": "MUTUAL_FUND", "performance_period": "1Y"},
        headers=auth_headers,
    )

    assert response.status_code == 200
    body = response.json()
    assert body["overview"]["selected_category"] == "Mutual Fund"
    assert body["overview"]["holdings_count"] == 1
    assert body["benchmark"]["price"] == 22500.0
    assert body["risk_metrics"]["beta"] == 0.8
