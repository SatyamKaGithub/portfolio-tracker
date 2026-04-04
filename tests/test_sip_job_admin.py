from datetime import date, datetime

from app.models import ImportedHolding, RecurringSip, SipJobRun
from app.services import get_sip_job_status, process_due_sips, run_sip_job


def test_process_due_sips_catches_up_missed_months(db_session, monkeypatch):
    today = date.today()
    start_month = 1 if today.month > 2 else 2
    past_start = date(today.year - 1, start_month, min(today.day, 20))

    db_session.add(
        ImportedHolding(
            user_id=1,
            symbol="AXISMF",
            company_name="Axis Flexicap",
            asset_type="MUTUAL_FUND",
            quantity=1,
            avg_buy_cost=100,
            invested_amount=100,
            current_price=100,
            current_value=100,
            one_day_change=0,
            unrealized_pnl=0,
            currency="INR",
            imported_at=datetime(2026, 1, 1, 9, 0, 0),
        )
    )
    db_session.add(
        RecurringSip(
            user_id=1,
            symbol="AXISMF",
            amount=100,
            start_date=past_start,
            next_run_date=past_start,
            day_of_month=past_start.day,
            active=1,
        )
    )
    db_session.commit()

    monkeypatch.setattr("app.services._refresh_single_imported_holding", lambda *_: None)
    monkeypatch.setattr("app.services._fetch_close_for_date", lambda *_: 100.0)
    monkeypatch.setattr("app.services._upsert_imported_portfolio_snapshot", lambda *args, **kwargs: True)

    result = process_due_sips(db_session, user_id=1)
    assert result["processed_sips"] >= 2

    sip = db_session.query(RecurringSip).filter_by(symbol="AXISMF").first()
    assert sip.next_run_date > today


def test_run_sip_job_is_idempotent_per_day(db_session, monkeypatch):
    db_session.add(
        ImportedHolding(
            user_id=1,
            symbol="PPFAS",
            company_name="Parag Parikh Flexi Cap",
            asset_type="MUTUAL_FUND",
            quantity=1,
            avg_buy_cost=100,
            invested_amount=100,
            current_price=100,
            current_value=100,
            one_day_change=0,
            unrealized_pnl=0,
            currency="INR",
            imported_at=datetime(2026, 1, 1, 9, 0, 0),
        )
    )
    db_session.add(
        RecurringSip(
            user_id=1,
            symbol="PPFAS",
            amount=500,
            start_date=date.today(),
            next_run_date=date.today(),
            day_of_month=date.today().day,
            active=1,
        )
    )
    db_session.commit()

    monkeypatch.setattr("app.services._refresh_single_imported_holding", lambda *_: None)
    monkeypatch.setattr("app.services._fetch_close_for_date", lambda *_: 100.0)
    monkeypatch.setattr("app.services._upsert_imported_portfolio_snapshot", lambda *args, **kwargs: True)

    first = run_sip_job(db_session, user_id=1, trigger="MANUAL", force=False)
    second = run_sip_job(db_session, user_id=1, trigger="MANUAL", force=False)

    assert first["status"] == "success"
    assert second["status"] == "skipped"

    logs = db_session.query(SipJobRun).all()
    assert len(logs) == 1
    assert logs[0].status == "SUCCESS"


def test_get_sip_job_status_includes_totals(db_session):
    today = date.today()
    db_session.add_all(
        [
            SipJobRun(
                user_id=1,
                run_date=today,
                trigger="MANUAL",
                status="SUCCESS",
                processed_sips=2,
                started_at=datetime.utcnow(),
                ended_at=datetime.utcnow(),
            ),
            SipJobRun(
                user_id=1,
                run_date=date(today.year - 1, today.month, min(today.day, 20)),
                trigger="SCHEDULED",
                status="FAILED",
                processed_sips=0,
                error_message="network",
                started_at=datetime.utcnow(),
                ended_at=datetime.utcnow(),
            ),
        ]
    )
    db_session.commit()

    status = get_sip_job_status(db_session, user_id=1)
    assert status["last_run"]["status"] == "SUCCESS"
    assert status["totals"]["runs"] == 2
    assert status["totals"]["successful_runs"] == 1
    assert status["totals"]["failed_runs"] == 1
    assert status["totals"]["processed_sips_total"] == 2


def test_admin_endpoints_expose_status_and_manual_run(client, db_session, monkeypatch, auth_headers):
    db_session.add(
        ImportedHolding(
            user_id=1,
            symbol="AXISMF",
            company_name="Axis Flexicap",
            asset_type="MUTUAL_FUND",
            quantity=1,
            avg_buy_cost=100,
            invested_amount=100,
            current_price=100,
            current_value=100,
            one_day_change=0,
            unrealized_pnl=0,
            currency="INR",
            imported_at=datetime(2026, 1, 1, 9, 0, 0),
        )
    )
    db_session.add(
        RecurringSip(
            user_id=1,
            symbol="AXISMF",
            amount=100,
            start_date=date.today(),
            next_run_date=date.today(),
            day_of_month=date.today().day,
            active=1,
        )
    )
    db_session.commit()

    monkeypatch.setattr("app.services._refresh_single_imported_holding", lambda *_: None)
    monkeypatch.setattr("app.services._fetch_close_for_date", lambda *_: 100.0)
    monkeypatch.setattr("app.services._upsert_imported_portfolio_snapshot", lambda *args, **kwargs: True)

    run_response = client.post("/admin/sips/run", headers=auth_headers)
    assert run_response.status_code == 200
    assert run_response.json()["status"] in {"success", "skipped"}

    status_response = client.get("/admin/sips/status", headers=auth_headers)
    assert status_response.status_code == 200
    body = status_response.json()
    assert "scheduler" in body
    assert "totals" in body
    assert body["totals"]["runs"] >= 1
