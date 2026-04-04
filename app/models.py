from sqlalchemy import Column, Integer, String, Float, Date, DateTime, UniqueConstraint
from app.db import Base
from datetime import date, datetime

class Holding(Base):
    __tablename__ = "holdings"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, index=True)
    symbol = Column(String, index=True)
    quantity = Column(Float)
    avg_price = Column(Float)

class Price(Base):
    __tablename__ = "prices"
    __table_args__ = (
        UniqueConstraint("symbol", "date", name="uq_prices_symbol_date"),
    )

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String, index=True)
    price = Column(Float)
    date = Column(Date, default=date.today)

class PortfolioSnapshot(Base):
    __tablename__ = "portfolio_snapshots"
    __table_args__ = (
        UniqueConstraint("user_id", "date", name="uq_portfolio_snapshots_user_date"),
    )

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, index=True)
    total_value = Column(Float)
    total_invested = Column(Float)
    pnl = Column(Float)
    date = Column(Date, default=date.today)


class ImportedPortfolioSnapshot(Base):
    __tablename__ = "imported_portfolio_snapshots"
    __table_args__ = (
        UniqueConstraint("user_id", "date", name="uq_imported_portfolio_snapshots_user_date"),
    )

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, index=True)
    total_value = Column(Float)
    total_invested = Column(Float)
    pnl = Column(Float)
    date = Column(Date, default=date.today)

class Transaction(Base):
    __tablename__ = "transactions"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, index=True)
    symbol = Column(String, index=True)
    quantity = Column(Float)
    price = Column(Float)
    type = Column(String)  # BUY or SELL
    date = Column(Date, default=date.today)


class ImportedHolding(Base):
    __tablename__ = "imported_holdings"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, index=True)
    symbol = Column(String, index=True)
    company_name = Column(String, nullable=True)
    isin = Column(String, nullable=True)
    asset_type = Column(String, default="STOCK", index=True)
    sector = Column(String, nullable=True)
    geography = Column(String, nullable=True)
    exchange_symbol = Column(String, nullable=True)
    quantity = Column(Float, default=0)
    avg_buy_cost = Column(Float, default=0)
    invested_amount = Column(Float, default=0)
    prev_close = Column(Float, nullable=True)
    current_price = Column(Float, nullable=True)
    current_value = Column(Float, nullable=True)
    one_day_change = Column(Float, nullable=True)
    unrealized_pnl = Column(Float, nullable=True)
    pe_ratio = Column(Float, nullable=True)
    currency = Column(String, default="INR")
    source_file = Column(String, nullable=True)
    imported_at = Column(DateTime, default=datetime.utcnow, index=True)


class ImportedHoldingTransaction(Base):
    __tablename__ = "imported_holding_transactions"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, index=True)
    symbol = Column(String, index=True)
    quantity = Column(Float)
    price = Column(Float)
    type = Column(String)  # BUY or SELL
    date = Column(Date, default=date.today)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)


class RecurringSip(Base):
    __tablename__ = "recurring_sips"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, index=True)
    symbol = Column(String, index=True)
    amount = Column(Float)
    start_date = Column(Date, default=date.today)
    next_run_date = Column(Date, default=date.today, index=True)
    day_of_month = Column(Integer)
    active = Column(Integer, default=1)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)


class SipJobRun(Base):
    __tablename__ = "sip_job_runs"
    __table_args__ = (
        UniqueConstraint("user_id", "run_date", name="uq_sip_job_runs_user_run_date"),
    )

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, index=True)
    run_date = Column(Date, default=date.today, index=True)
    trigger = Column(String, default="SCHEDULED")
    status = Column(String, default="PENDING")  # PENDING/RUNNING/SUCCESS/FAILED/SKIPPED
    processed_sips = Column(Integer, default=0)
    skip_reason = Column(String, nullable=True)
    error_message = Column(String, nullable=True)
    started_at = Column(DateTime, default=datetime.utcnow, index=True)
    ended_at = Column(DateTime, nullable=True)


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True)
    email = Column(String, unique=True, index=True)
    password_hash = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)


class UserSession(Base):
    __tablename__ = "user_sessions"
    __table_args__ = (
        UniqueConstraint("token", name="uq_user_sessions_token"),
    )

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, index=True)
    token = Column(String, index=True)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    expires_at = Column(DateTime, index=True)


class PriceAlert(Base):
    __tablename__ = "price_alerts"
    __table_args__ = (
        UniqueConstraint("id", "user_id", name="uq_price_alerts_id_user_id"),
    )

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, index=True)
    symbol = Column(String, index=True)
    target_price = Column(Float)
    direction = Column(String, default="ABOVE")  # ABOVE or BELOW
    duration = Column(String, default="UNTIL_HIT")  # 1_WEEK / 1_MONTH / 3_MONTHS / UNTIL_HIT
    channel = Column(String, default="IN_APP")  # IN_APP / EMAIL / BOTH
    status = Column(String, default="ACTIVE")  # ACTIVE / TRIGGERED / EXPIRED / DISABLED
    note = Column(String, nullable=True)
    last_checked_price = Column(Float, nullable=True)
    triggered_price = Column(Float, nullable=True)
    triggered_at = Column(DateTime, nullable=True, index=True)
    expires_at = Column(DateTime, nullable=True, index=True)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, index=True)


class AlertNotification(Base):
    __tablename__ = "alert_notifications"
    __table_args__ = (
        UniqueConstraint("id", "user_id", name="uq_alert_notifications_id_user_id"),
    )

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, index=True)
    alert_id = Column(Integer, index=True)
    channel = Column(String, default="IN_APP")  # IN_APP / EMAIL
    title = Column(String)
    message = Column(String)
    delivery_status = Column(String, default="CREATED")  # CREATED / SENT / FAILED / SKIPPED
    read_at = Column(DateTime, nullable=True, index=True)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
