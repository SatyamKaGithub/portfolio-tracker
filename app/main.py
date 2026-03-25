from contextlib import asynccontextmanager

from fastapi import FastAPI, Depends, Header, HTTPException, Query
from sqlalchemy.orm import Session
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Union
from app.db import engine, SessionLocal, Base
from app.models import Holding, PortfolioSnapshot
from app.schemas import (
    HoldingCreate,
    HoldingsImportPayload,
    ImportedHoldingTransactionCreate,
    LoginPayload,
    RecurringSipCreate,
    SignupPayload,
)
from app.services import (
    apply_imported_holding_transaction,
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
    get_nifty50_ticker_snapshot,
    get_sip_job_status,
    get_user_from_token,
    import_holdings_workbook,
    login_user_account,
    logout_user_session,
    run_sip_job,
    refresh_imported_holdings_market_data,
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


@app.get("/")
def root():
    return {"message": "Portfolio Tracker API is running"}

@app.post("/holdings")
def create_holding(holding: HoldingCreate, db: Session = Depends(get_db)):
    db_holding = Holding(
        symbol=holding.symbol.strip().upper(),
        quantity=holding.quantity,
        avg_price=holding.avg_price
    )
    db.add(db_holding)
    db.commit()
    db.refresh(db_holding)
    return db_holding

@app.get("/holdings")
def get_holdings(db: Session = Depends(get_db)):
    return db.query(Holding).all()
@app.get("/portfolio/value")
def get_portfolio_value(db: Session = Depends(get_db)):
    return calculate_portfolio_value(db)

@app.post("/prices/update")
def refresh_prices(db: Session = Depends(get_db)):
    result = update_prices(db)
    return {"message": "Prices updated", **result}

@app.get("/portfolio/history")
def get_portfolio_history(db: Session = Depends(get_db)):
    return db.query(PortfolioSnapshot).order_by(PortfolioSnapshot.date).all()

@app.get("/portfolio/performance")
def get_portfolio_performance(db: Session = Depends(get_db)):
    return calculate_performance_metrics(db)


@app.get("/portfolio/daily-returns")
def get_portfolio_daily_returns(
    limit: int = Query(default=30, ge=1, le=5000),
    db: Session = Depends(get_db)
):
    return calculate_daily_returns(db, limit=limit)
    
@app.get("/portfolio/drawdown")
def get_portfolio_drawdown(db: Session = Depends(get_db)):
    return calculate_max_drawdown(db)

@app.get("/portfolio/volatility")
def get_portfolio_volatility(db: Session = Depends(get_db)):
    return calculate_volatility(db)

@app.get("/portfolio/sharpe")
def get_portfolio_sharpe(db: Session = Depends(get_db)):
    return calculate_sharpe_ratio(db)



@app.get("/portfolio/rolling-volatility")
def get_rolling_volatility(
    window: int = Query(default=3, ge=2, le=252),
    db: Session = Depends(get_db)
):
    return calculate_rolling_volatility(db, window)



@app.get("/portfolio/beta")
def get_portfolio_beta(benchmark: str = "^NSEI", db: Session = Depends(get_db)):
    return calculate_beta(db, benchmark)


@app.get("/portfolio/alpha")
def get_portfolio_alpha(
    benchmark: str = "^NSEI",
    db: Session = Depends(get_db)
):
    return calculate_alpha(db, benchmark)


@app.get("/portfolio/information-ratio")
def get_portfolio_information_ratio(
    benchmark: str = "^NSEI",
    db: Session = Depends(get_db)
):
    return calculate_information_ratio(db, benchmark)


@app.get("/portfolio/tracking-error")
def get_portfolio_tracking_error(
    benchmark: str = "^NSEI",
    db: Session = Depends(get_db)
):
    return calculate_tracking_error(db, benchmark)

@app.post("/transactions")
def add_transaction(
    txn: Union[TransactionCreate, List[TransactionCreate]],
    db: Session = Depends(get_db),
):
    try:
        if isinstance(txn, list):
            return create_transactions(db, txn)
        return create_transaction(db, txn)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

@app.get("/transactions")
def get_transactions(db: Session = Depends(get_db)):
    return db.query(Transaction).order_by(Transaction.date.asc(), Transaction.id.asc()).all()


@app.post("/imports/holdings")
def import_holdings(payload: HoldingsImportPayload, db: Session = Depends(get_db)):
    try:
        return import_holdings_workbook(db, payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/imports/holdings/refresh")
def refresh_imported_holdings(db: Session = Depends(get_db)):
    return refresh_imported_holdings_market_data(db)


@app.post("/imports/holdings/transactions")
def apply_imported_transaction(
    txn: ImportedHoldingTransactionCreate,
    db: Session = Depends(get_db),
):
    try:
        return apply_imported_holding_transaction(db, txn)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/imports/holdings/sips")
def add_recurring_sip(
    payload: RecurringSipCreate,
    db: Session = Depends(get_db),
):
    try:
        return create_recurring_sip(db, payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/portfolio/imported-dashboard")
def imported_dashboard(
    category: str = Query(default="ALL"),
    performance_period: str = Query(default="1Y"),
    db: Session = Depends(get_db),
):
    return get_imported_portfolio_dashboard(
        db,
        category=category,
        performance_period=performance_period,
    )


@app.get("/admin/sips/status")
def sip_job_status(db: Session = Depends(get_db)):
    status = get_sip_job_status(db)
    status["scheduler"] = get_sip_scheduler_status()
    return status


@app.post("/admin/sips/run")
def run_sip_processing_job(
    force: bool = Query(default=False),
    db: Session = Depends(get_db),
):
    try:
        return run_sip_job(db, trigger="MANUAL", force=force)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/market/nifty50")
def nifty50_snapshot():
    return get_nifty50_ticker_snapshot()


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
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    token = authorization.replace("Bearer ", "").strip() if authorization else ""
    user = get_user_from_token(db, token)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid or expired session")
    return user


@app.post("/auth/logout")
def logout(
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    token = authorization.replace("Bearer ", "").strip() if authorization else ""
    if not token:
        raise HTTPException(status_code=401, detail="Missing session token")
    if not logout_user_session(db, token):
        raise HTTPException(status_code=401, detail="Invalid or expired session")
    return {"message": "Logged out successfully"}
