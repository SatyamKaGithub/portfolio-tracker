from sqlalchemy import Column, Integer, String, Float, Date
from app.db import Base
from datetime import date

class Holding(Base):
    __tablename__ = "holdings"

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String, index=True)
    quantity = Column(Float)
    avg_price = Column(Float)

class Price(Base):
    __tablename__ = "prices"

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String, index=True)
    price = Column(Float)
    date = Column(Date, default=date.today)