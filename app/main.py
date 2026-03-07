from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Union
from app.db import engine, SessionLocal, Base
from app.models import Holding, PortfolioSnapshot
from app.schemas import HoldingCreate
from app.services import (
    calculate_alpha,
    calculate_beta,
    calculate_max_drawdown,
    calculate_performance_metrics,
    calculate_portfolio_value,
    calculate_rolling_volatility,
    calculate_sharpe_ratio,
    calculate_volatility,
    update_prices,
)
from app.schemas import TransactionCreate
from app.models import Transaction
from app.services import create_transaction, create_transactions


app = FastAPI()
Base.metadata.create_all(bind=engine)

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
    return db.query(Transaction).order_by(Transaction.date).all()
