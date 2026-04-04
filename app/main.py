from contextlib import asynccontextmanager

from fastapi import FastAPI, Depends, Header, HTTPException, Query
from sqlalchemy.orm import Session
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Union
from app.db import engine, SessionLocal, Base, ensure_compatible_schema
from app.models import Holding, PortfolioSnapshot
from app.schemas import (
    PriceAlertCreate,
    HoldingCreate,
    HoldingsImportPayload,
    ImportedHoldingTransactionCreate,
    LoginPayload,
    RecurringSipCreate,
    SignupPayload,
)
from app.services import (
    apply_imported_holding_transaction,
    create_price_alert,
    calculate_alpha,
    calculate_beta,
    calculate_daily_returns,
    calculate_information_ratio,
    calculate_max_drawdown,
    calculate_performance_metrics,
    calculate_portfolio_value,
    calculate_rolling_volatility,
    calculate_sharpe_ratio,
    calculate_tracking_error,
    calculate_volatility,
    create_recurring_sip,
    create_user_account,
    get_imported_portfolio_dashboard,
    list_user_alert_notifications,
    list_user_price_alerts,
    mark_alert_notification_read,
    get_nifty50_ticker_snapshot,
    get_sip_job_status,
    get_user_from_token,
    import_holdings_workbook,
    login_user_account,
    logout_user_session,
    run_sip_job,
    refresh_imported_holdings_market_data,
    run_price_alert_check_job,
    update_prices,
)
from app.schemas import TransactionCreate
from app.models import Transaction
from app.services import create_transaction, create_transactions
from app.scheduler import get_sip_scheduler_status, start_sip_scheduler, stop_sip_scheduler


@asynccontextmanager
async def lifespan(_app: FastAPI):
    start_sip_scheduler()
    try:
        yield
    finally:
        stop_sip_scheduler()


app = FastAPI(lifespan=lifespan)
Base.metadata.create_all(bind=engine)
ensure_compatible_schema(engine)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Dependency to get DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def _get_token_from_authorization(authorization: str | None) -> str:
    return authorization.replace("Bearer ", "").strip() if authorization else ""


def get_current_user(
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    token = _get_token_from_authorization(authorization)
    user = get_user_from_token(db, token)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid or expired session")
    return user


@app.get("/")
def root():
    return {"message": "Portfolio Tracker API is running"}

@app.post("/holdings")
def create_holding(
    holding: HoldingCreate,
    user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    db_holding = Holding(
        user_id=int(user["id"]),
        symbol=holding.symbol.strip().upper(),
        quantity=holding.quantity,
        avg_price=holding.avg_price
    )
    db.add(db_holding)
    db.commit()
    db.refresh(db_holding)
    return db_holding

@app.get("/holdings")
def get_holdings(
    user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    return db.query(Holding).filter(Holding.user_id == int(user["id"])).all()
@app.get("/portfolio/value")
def get_portfolio_value(
    user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    return calculate_portfolio_value(db, user_id=int(user["id"]))

@app.post("/prices/update")
def refresh_prices(
    user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    result = update_prices(db, user_id=int(user["id"]))
    return {"message": "Prices updated", **result}

@app.get("/portfolio/history")
def get_portfolio_history(
    user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    return db.query(PortfolioSnapshot).filter(
        PortfolioSnapshot.user_id == int(user["id"])
    ).order_by(PortfolioSnapshot.date).all()

@app.get("/portfolio/performance")
def get_portfolio_performance(
    user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    return calculate_performance_metrics(db, user_id=int(user["id"]))


@app.get("/portfolio/daily-returns")
def get_portfolio_daily_returns(
    limit: int = Query(default=30, ge=1, le=5000),
    user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    return calculate_daily_returns(db, limit=limit, user_id=int(user["id"]))
    
@app.get("/portfolio/drawdown")
def get_portfolio_drawdown(
    user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    return calculate_max_drawdown(db, user_id=int(user["id"]))

@app.get("/portfolio/volatility")
def get_portfolio_volatility(
    user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    return calculate_volatility(db, user_id=int(user["id"]))

@app.get("/portfolio/sharpe")
def get_portfolio_sharpe(
    user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    return calculate_sharpe_ratio(db, user_id=int(user["id"]))



@app.get("/portfolio/rolling-volatility")
def get_rolling_volatility(
    window: int = Query(default=3, ge=2, le=252),
    user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    return calculate_rolling_volatility(db, window, user_id=int(user["id"]))



@app.get("/portfolio/beta")
def get_portfolio_beta(
    benchmark: str = "^NSEI",
    user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    return calculate_beta(db, benchmark, user_id=int(user["id"]))


@app.get("/portfolio/alpha")
def get_portfolio_alpha(
    benchmark: str = "^NSEI",
    user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    return calculate_alpha(db, benchmark, user_id=int(user["id"]))


@app.get("/portfolio/information-ratio")
def get_portfolio_information_ratio(
    benchmark: str = "^NSEI",
    user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    return calculate_information_ratio(db, benchmark, user_id=int(user["id"]))


@app.get("/portfolio/tracking-error")
def get_portfolio_tracking_error(
    benchmark: str = "^NSEI",
    user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    return calculate_tracking_error(db, benchmark, user_id=int(user["id"]))

@app.post("/transactions")
def add_transaction(
    txn: Union[TransactionCreate, List[TransactionCreate]],
    user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    try:
        if isinstance(txn, list):
            return create_transactions(db, txn, user_id=int(user["id"]))
        return create_transaction(db, txn, user_id=int(user["id"]))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

@app.get("/transactions")
def get_transactions(
    user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    return db.query(Transaction).filter(
        Transaction.user_id == int(user["id"])
    ).order_by(Transaction.date.asc(), Transaction.id.asc()).all()


@app.post("/imports/holdings")
def import_holdings(
    payload: HoldingsImportPayload,
    user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    try:
        return import_holdings_workbook(db, payload, user_id=int(user["id"]))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/imports/holdings/refresh")
def refresh_imported_holdings(
    user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    return refresh_imported_holdings_market_data(db, user_id=int(user["id"]))


@app.post("/imports/holdings/transactions")
def apply_imported_transaction(
    txn: ImportedHoldingTransactionCreate,
    user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    try:
        return apply_imported_holding_transaction(db, txn, user_id=int(user["id"]))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/imports/holdings/sips")
def add_recurring_sip(
    payload: RecurringSipCreate,
    user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    try:
        return create_recurring_sip(db, payload, user_id=int(user["id"]))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/portfolio/imported-dashboard")
def imported_dashboard(
    category: str = Query(default="ALL"),
    performance_period: str = Query(default="1Y"),
    user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    return get_imported_portfolio_dashboard(
        db,
        category=category,
        performance_period=performance_period,
        user_id=int(user["id"]),
    )


@app.get("/admin/sips/status")
def sip_job_status(
    user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    status = get_sip_job_status(db, user_id=int(user["id"]))
    status["scheduler"] = get_sip_scheduler_status()
    return status


@app.post("/admin/sips/run")
def run_sip_processing_job(
    force: bool = Query(default=False),
    user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    try:
        return run_sip_job(db, user_id=int(user["id"]), trigger="MANUAL", force=force)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/market/nifty50")
def nifty50_snapshot():
    return get_nifty50_ticker_snapshot()


@app.post("/alerts/price")
def add_price_alert(
    payload: PriceAlertCreate,
    user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    try:
        return create_price_alert(db, user_id=int(user["id"]), payload=payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/alerts/price")
def get_price_alerts(
    include_inactive: bool = Query(default=False),
    user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    return list_user_price_alerts(
        db,
        user_id=int(user["id"]),
        include_inactive=include_inactive,
    )


@app.post("/alerts/price/check")
def run_price_alert_check(
    user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    return run_price_alert_check_job(db, user_id=int(user["id"]))


@app.get("/notifications")
def get_notifications(
    limit: int = Query(default=50, ge=1, le=200),
    unread_only: bool = Query(default=False),
    user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    return list_user_alert_notifications(
        db,
        user_id=int(user["id"]),
        limit=limit,
        unread_only=unread_only,
    )


@app.post("/notifications/{notification_id}/read")
def read_notification(
    notification_id: int,
    user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    try:
        return mark_alert_notification_read(
            db,
            user_id=int(user["id"]),
            notification_id=notification_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post("/auth/signup")
def signup(payload: SignupPayload, db: Session = Depends(get_db)):
    try:
        return create_user_account(db, payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/auth/login")
def login(payload: LoginPayload, db: Session = Depends(get_db)):
    try:
        return login_user_account(db, payload)
    except ValueError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc


@app.get("/auth/me")
def me(
    user: dict = Depends(get_current_user),
):
    return user


@app.post("/auth/logout")
def logout(
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    token = _get_token_from_authorization(authorization)
    if not token:
        raise HTTPException(status_code=401, detail="Missing session token")
    if not logout_user_session(db, token):
        raise HTTPException(status_code=401, detail="Invalid or expired session")
    return {"message": "Logged out successfully"}
