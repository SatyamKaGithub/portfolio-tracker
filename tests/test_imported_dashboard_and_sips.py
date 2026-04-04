from datetime import date, datetime

from app.models import ImportedHolding, ImportedHoldingTransaction, RecurringSip
from app.schemas import RecurringSipCreate
from app.services import create_recurring_sip, get_imported_portfolio_dashboard, process_due_sips


def test_process_due_sips_creates_transaction_and_updates_next_run_date(db_session, monkeypatch):
    today = date.today()

    holding = ImportedHolding(
        user_id=1,
        symbol="AXISMF",
        company_name="Axis Flexicap",
        asset_type="MUTUAL_FUND",
        quantity=10,
        avg_buy_cost=100,
        invested_amount=1000,
        current_price=100,
        prev_close=99,
        current_value=1000,
        one_day_change=10,
        unrealized_pnl=0,
        currency="INR",
        imported_at=datetime(2026, 1, 1, 9, 0, 0),
    )
    sip = RecurringSip(
        user_id=1,
        symbol="AXISMF",
        amount=500,
        start_date=today,
        next_run_date=today,
        day_of_month=today.day,
        active=1,
    )
    db_session.add_all([holding, sip])
    db_session.commit()

    monkeypatch.setattr("app.services._refresh_single_imported_holding", lambda *_: None)
    monkeypatch.setattr("app.services._fetch_close_for_date", lambda *_: 100.0)
    monkeypatch.setattr("app.services._upsert_imported_portfolio_snapshot", lambda *args, **kwargs: True)

    result = process_due_sips(db_session, user_id=1)
    assert result["processed_sips"] == 1

    updated_holding = db_session.query(ImportedHolding).filter_by(symbol="AXISMF").first()
    assert round(updated_holding.quantity, 2) == 15
    assert round(updated_holding.invested_amount, 2) == 1500

    txns = db_session.query(ImportedHoldingTransaction).all()
    assert len(txns) == 1
    assert txns[0].type == "SIP_BUY"
    assert round(txns[0].quantity, 2) == 5

    updated_sip = db_session.query(RecurringSip).filter_by(symbol="AXISMF").first()
    assert updated_sip.next_run_date > today


def test_create_recurring_sip_creates_schedule_and_triggers_processing(db_session, monkeypatch):
    holding = ImportedHolding(
        user_id=1,
        symbol="PPFAS",
        company_name="Parag Parikh Flexi Cap",
        asset_type="MUTUAL_FUND",
        quantity=2,
        avg_buy_cost=50,
        invested_amount=100,
        current_price=55,
        current_value=110,
        one_day_change=0,
        unrealized_pnl=10,
        currency="INR",
        imported_at=datetime(2026, 1, 1, 9, 0, 0),
    )
    db_session.add(holding)
    db_session.commit()

    calls = {"count": 0}

    def fake_process_due_sips(*args, **kwargs):
        calls["count"] += 1
        return {"processed_sips": 0}

    monkeypatch.setattr("app.services.process_due_sips", fake_process_due_sips)

    payload = RecurringSipCreate(symbol="PPFAS", amount=2500, start_date=date(2026, 1, 10))
    result = create_recurring_sip(db_session, payload, user_id=1)

    assert result["symbol"] == "PPFAS"
    assert result["amount"] == 2500
    assert result["next_run_date"] == "2026-01-10"
    assert result["active"] is True
    assert calls["count"] == 1


def test_create_recurring_sip_rejects_non_mutual_fund(db_session):
    db_session.add(
        ImportedHolding(
            user_id=1,
            symbol="INFY",
            company_name="Infosys",
            asset_type="STOCK",
            quantity=1,
            avg_buy_cost=1000,
            invested_amount=1000,
            current_price=1100,
            current_value=1100,
            one_day_change=0,
            unrealized_pnl=100,
            currency="INR",
            imported_at=datetime(2026, 1, 1, 9, 0, 0),
        )
    )
    db_session.commit()

    payload = RecurringSipCreate(symbol="INFY", amount=500, start_date=date(2026, 2, 1))

    try:
        create_recurring_sip(db_session, payload, user_id=1)
        assert False, "Expected ValueError"
    except ValueError as exc:
        assert "currently supported only for mutual funds" in str(exc)


def test_imported_dashboard_uses_stubbed_market_data_and_filters_category(db_session, monkeypatch):
    db_session.add_all(
        [
            ImportedHolding(
                user_id=1,
                symbol="HDFCBANK",
                company_name="HDFC Bank",
                asset_type="STOCK",
                sector="Financials",
                quantity=2,
                avg_buy_cost=1000,
                invested_amount=2000,
                current_price=1100,
                prev_close=1080,
                current_value=2200,
                one_day_change=40,
                unrealized_pnl=200,
                pe_ratio=20,
                currency="INR",
                source_file="holdings.xlsx",
                imported_at=datetime(2026, 1, 5, 10, 0, 0),
            ),
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
                pe_ratio=None,
                currency="INR",
                source_file="holdings.xlsx",
                imported_at=datetime(2026, 1, 6, 10, 0, 0),
            ),
            RecurringSip(
                user_id=1,
                symbol="AXISMF",
                amount=2000,
                start_date=date(2026, 1, 6),
                next_run_date=date(2026, 2, 6),
                day_of_month=6,
                active=1,
            ),
        ]
    )
    db_session.commit()

    monkeypatch.setattr("app.services._ensure_imported_snapshot_history", lambda *args, **kwargs: None)
    monkeypatch.setattr("app.services.process_due_sips", lambda *args, **kwargs: {"processed_sips": 0})
    monkeypatch.setattr(
        "app.services._fetch_benchmark_mini_chart",
        lambda symbol, name: {
            "symbol": symbol,
            "name": name,
            "current_level": 22500.0 if symbol == "^NSEI" else 74000.0,
            "prev_close": 22400.0 if symbol == "^NSEI" else 73800.0,
            "points_change": 100.0 if symbol == "^NSEI" else 200.0,
            "change_percent": 0.45 if symbol == "^NSEI" else 0.27,
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
            "pe_ratio": 22.0,
        },
    )
    monkeypatch.setattr(
        "app.services._calculate_imported_risk_metrics",
        lambda *args, **kwargs: {
            "sharpe_ratio": 1.5,
            "beta": 0.9,
            "alpha_annualized_percent": 3.2,
            "observations": 42,
        },
    )
    monkeypatch.setattr(
        "app.services._build_normalized_performance_comparison",
        lambda *args, **kwargs: {
            "benchmark": "^NSEI",
            "points": [{"date": "2026-01-06", "portfolio_value": 100.0, "benchmark_value": 100.0, "portfolio_change_percent": 0.0, "benchmark_change_percent": 0.0}],
            "start_date": "2026-01-06",
            "end_date": "2026-01-06",
            "observations": 1,
        },
    )

    dashboard = get_imported_portfolio_dashboard(db_session, category="ALL", performance_period="1Y", user_id=1)

    assert dashboard["overview"]["total_net_worth"] == 2800
    assert dashboard["overview"]["holdings_count"] == 2
    assert "Mutual Fund" in dashboard["overview"]["available_categories"]
    assert dashboard["benchmark"]["price"] == 22500.0
    assert dashboard["benchmark"]["one_day_change_percent"] == 0.45
    assert dashboard["portfolio_avg_pe"] == 20.0
    assert dashboard["benchmark_pe_gap"] == -2.0
    assert len(dashboard["recurring_sips"]) == 1

    mf_dashboard = get_imported_portfolio_dashboard(db_session, category="MUTUAL_FUND", performance_period="1Y", user_id=1)
    assert mf_dashboard["overview"]["holdings_count"] == 1
    assert mf_dashboard["holdings"][0]["symbol"] == "AXISMF"
