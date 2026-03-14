from pydantic import BaseModel, field_validator
from datetime import date as dt_date
from typing import Optional, Literal, List


class HoldingCreate(BaseModel):
    symbol: str
    quantity: float
    avg_price: float


class TransactionCreate(BaseModel):
    symbol: str
    quantity: float
    price: float
    type: Literal["BUY", "SELL"]
    date: Optional[dt_date] = None

    @field_validator("symbol")
    @classmethod
    def validate_symbol(cls, value: str) -> str:
        normalized = value.strip().upper()
        if not normalized:
            raise ValueError("symbol is required")
        return normalized

    @field_validator("quantity", "price")
    @classmethod
    def validate_positive_numbers(cls, value: float) -> float:
        if value <= 0:
            raise ValueError("quantity and price must be greater than zero")
        return value

    @field_validator("type", mode="before")
    @classmethod
    def normalize_type(cls, value: str) -> str:
        return value.strip().upper()

    @field_validator("date")
    @classmethod
    def validate_date(cls, value: Optional[dt_date]) -> Optional[dt_date]:
        if value and value > dt_date.today():
            raise ValueError("transaction date cannot be in the future")
        return value


class HoldingsImportPayload(BaseModel):
    filename: str
    content_base64: str


class ImportedHoldingTransactionCreate(BaseModel):
    symbol: str
    quantity: float
    price: float
    type: Literal["BUY", "SELL"]
    date: Optional[dt_date] = None

    @field_validator("symbol")
    @classmethod
    def validate_imported_symbol(cls, value: str) -> str:
        normalized = value.strip().upper()
        if not normalized:
            raise ValueError("symbol is required")
        return normalized

    @field_validator("quantity", "price")
    @classmethod
    def validate_imported_positive_numbers(cls, value: float) -> float:
        if value <= 0:
            raise ValueError("quantity and price must be greater than zero")
        return value

    @field_validator("type", mode="before")
    @classmethod
    def normalize_imported_type(cls, value: str) -> str:
        return value.strip().upper()

    @field_validator("date")
    @classmethod
    def validate_imported_date(cls, value: Optional[dt_date]) -> Optional[dt_date]:
        if value and value > dt_date.today():
            raise ValueError("transaction date cannot be in the future")
        return value


class RecurringSipCreate(BaseModel):
    symbol: str
    amount: float
    start_date: dt_date

    @field_validator("symbol")
    @classmethod
    def validate_sip_symbol(cls, value: str) -> str:
        normalized = value.strip().upper()
        if not normalized:
            raise ValueError("symbol is required")
        return normalized

    @field_validator("amount")
    @classmethod
    def validate_sip_amount(cls, value: float) -> float:
        if value <= 0:
            raise ValueError("amount must be greater than zero")
        return value

    @field_validator("start_date")
    @classmethod
    def validate_sip_start_date(cls, value: dt_date) -> dt_date:
        if value.year < 2000:
            raise ValueError("start date looks invalid")
        return value


class ImportedHoldingRow(BaseModel):
    symbol: str
    company_name: Optional[str] = None
    isin: Optional[str] = None
    asset_type: str = "STOCK"
    sector: Optional[str] = None
    geography: Optional[str] = None
    exchange_symbol: Optional[str] = None
    quantity: float
    avg_buy_cost: float
    invested_amount: float
    prev_close: Optional[float] = None
    current_price: Optional[float] = None
    current_value: Optional[float] = None
    one_day_change: Optional[float] = None
    unrealized_pnl: Optional[float] = None
    pe_ratio: Optional[float] = None
    currency: str = "INR"
    source_file: Optional[str] = None


class AllocationBucket(BaseModel):
    name: str
    value: float
    weight_percent: float


class BenchmarkSummary(BaseModel):
    symbol: str
    name: str
    price: Optional[float] = None
    prev_close: Optional[float] = None
    one_day_change_percent: Optional[float] = None
    pe_ratio: Optional[float] = None


class BenchmarkChartPoint(BaseModel):
    date: str
    value: float


class BenchmarkMiniChart(BaseModel):
    symbol: str
    name: str
    current_level: Optional[float] = None
    prev_close: Optional[float] = None
    points_change: Optional[float] = None
    change_percent: Optional[float] = None
    trend: Optional[str] = None
    points: List[BenchmarkChartPoint]


class RecurringSipSummary(BaseModel):
    id: int
    symbol: str
    amount: float
    start_date: str
    next_run_date: str
    day_of_month: int
    active: bool


class RiskMetrics(BaseModel):
    sharpe_ratio: Optional[float] = None
    beta: Optional[float] = None
    alpha_annualized_percent: Optional[float] = None
    observations: int = 0


class PerformancePoint(BaseModel):
    date: str
    portfolio_value: float
    benchmark_value: float
    portfolio_change_percent: float
    benchmark_change_percent: float


class PerformanceComparison(BaseModel):
    benchmark: str
    points: List[PerformancePoint]
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    observations: int = 0


class ImportedPortfolioOverview(BaseModel):
    total_net_worth: float
    total_gain: float
    total_gain_percent: float
    one_day_change: float
    one_day_change_percent: float
    holdings_count: int
    as_of: Optional[str] = None
    selected_category: str
    available_categories: List[str]


class ImportedPortfolioDashboard(BaseModel):
    overview: ImportedPortfolioOverview
    holdings: List[ImportedHoldingRow]
    asset_allocation: List[AllocationBucket]
    sector_allocation: List[AllocationBucket]
    benchmark: BenchmarkSummary
    benchmark_charts: List[BenchmarkMiniChart]
    risk_metrics: RiskMetrics
    performance_comparison: PerformanceComparison
    recurring_sips: List[RecurringSipSummary]
    portfolio_avg_pe: Optional[float] = None
    benchmark_pe_gap: Optional[float] = None
    import_file_name: Optional[str] = None
    imported_at: Optional[str] = None
