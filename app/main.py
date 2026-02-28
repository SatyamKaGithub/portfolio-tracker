from app.services import calculate_portfolio_value, update_prices
from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
from app.db import engine, SessionLocal
from app.models import Holding
from app.schemas import HoldingCreate
from app.models import PortfolioSnapshot
from app.services import calculate_performance_metrics
from app.services import calculate_max_drawdown
from app.services import calculate_volatility
from app.services import calculate_sharpe_ratio
from app.services import calculate_rolling_volatility
from app.services import calculate_beta

app = FastAPI()

Holding.metadata.create_all(bind=engine)

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
        symbol=holding.symbol,
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
    update_prices(db)
    return {"message": "Prices updated"}

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
def get_rolling_volatility(window: int = 3, db: Session = Depends(get_db)):
    return calculate_rolling_volatility(db, window)



@app.get("/portfolio/beta")
def get_portfolio_beta(benchmark: str = "^NSEI", db: Session = Depends(get_db)):
    return calculate_beta(db, benchmark)