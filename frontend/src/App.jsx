import { useCallback, useEffect, useMemo, useState } from "react"
import {
  applyImportedHoldingTransaction,
  addRecurringSip,
  getImportedDashboard,
  importHoldingsWorkbook,
  refreshImportedHoldings
} from "./services/api"
import "./App.css"

const PIE_COLORS = [
  "#4E79A7",
  "#59A14F",
  "#E15759",
  "#76B7B2",
  "#B07AA1",
  "#F28E2B",
  "#9C755F",
  "#EDC948"
]

function formatCurrency(value) {
  const amount = Number(value)
  return new Intl.NumberFormat("en-IN", {
    style: "currency",
    currency: "INR",
    maximumFractionDigits: 2
  }).format(Number.isFinite(amount) ? amount : 0)
}

function formatNumber(value, digits = 2) {
  const amount = Number(value)
  if (!Number.isFinite(amount)) {
    return "N/A"
  }

  return new Intl.NumberFormat("en-IN", {
    maximumFractionDigits: digits,
    minimumFractionDigits: digits
  }).format(amount)
}

function formatPercent(value, digits = 2) {
  const amount = Number(value)
  if (!Number.isFinite(amount)) {
    return "N/A"
  }

  return `${amount >= 0 ? "+" : ""}${formatNumber(amount, digits)}%`
}

function formatDateTime(value) {
  if (!value) {
    return "N/A"
  }

  const date = new Date(value)
  if (Number.isNaN(date.getTime())) {
    return value
  }

  return new Intl.DateTimeFormat("en-IN", {
    dateStyle: "medium",
    timeStyle: "short"
  }).format(date)
}

function todayInputValue() {
  const now = new Date()
  const localTime = new Date(now.getTime() - now.getTimezoneOffset() * 60000)
  return localTime.toISOString().slice(0, 10)
}

function formatDateLabel(value) {
  if (!value) {
    return ""
  }

  const date = new Date(value)
  if (Number.isNaN(date.getTime())) {
    return value
  }

  return new Intl.DateTimeFormat("en-IN", {
    day: "2-digit",
    month: "short"
  }).format(date)
}

function statusTone(value) {
  return Number(value) >= 0 ? "positive" : "negative"
}

function polarToCartesian(centerX, centerY, radius, angleDeg) {
  const angleRad = ((angleDeg - 90) * Math.PI) / 180
  return {
    x: centerX + radius * Math.cos(angleRad),
    y: centerY + radius * Math.sin(angleRad)
  }
}

function describeSector(centerX, centerY, radius, startAngle, endAngle) {
  const start = polarToCartesian(centerX, centerY, radius, endAngle)
  const end = polarToCartesian(centerX, centerY, radius, startAngle)
  const largeArcFlag = endAngle - startAngle <= 180 ? "0" : "1"
  return [
    "M",
    centerX,
    centerY,
    "L",
    start.x,
    start.y,
    "A",
    radius,
    radius,
    0,
    largeArcFlag,
    0,
    end.x,
    end.y,
    "Z"
  ].join(" ")
}

async function fileToBase64(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader()
    reader.onload = () => {
      const result = typeof reader.result === "string" ? reader.result : ""
      const [, payload = ""] = result.split(",")
      resolve(payload)
    }
    reader.onerror = () => reject(new Error("Failed to read file"))
    reader.readAsDataURL(file)
  })
}

function AllocationPie({
  title,
  subtitle,
  items,
  filterType,
  activeFilter,
  onSliceSelect
}) {
  const total = items.reduce((sum, item) => sum + Number(item.value || 0), 0)
  const [hovered, setHovered] = useState(null)

  if (!items.length || total <= 0) {
    return (
      <article className="panel">
        <div className="panel-heading">
          <div>
            <p className="panel-kicker">{subtitle}</p>
            <h3>{title}</h3>
          </div>
        </div>
        <p className="empty-state">No allocation data yet.</p>
      </article>
    )
  }

  const slices = items.reduce((accumulator, item, index) => {
    const previousAngle =
      accumulator.length > 0 ? accumulator[accumulator.length - 1].endAngle : 0
    const portion = Number(item.value || 0) / total
    const endAngle = previousAngle + portion * 360
    const isActive =
      activeFilter?.type === filterType && activeFilter?.value === item.name

    accumulator.push({
      ...item,
      color: PIE_COLORS[index % PIE_COLORS.length],
      path: describeSector(120, 120, 88, previousAngle, endAngle),
      startAngle: previousAngle,
      endAngle,
      isActive
    })

    return accumulator
  }, [])

  const focusItem = hovered || slices.find((item) => item.isActive) || slices[0]

  return (
    <article className="panel pie-panel">
      <div className="panel-heading">
        <div>
          <p className="panel-kicker">{subtitle}</p>
          <h3>{title}</h3>
        </div>
        {activeFilter?.type === filterType ? (
          <button className="ghost-button" type="button" onClick={() => onSliceSelect(null)}>
            Back to all
          </button>
        ) : null}
      </div>

      <div className="pie-layout">
        <div className="pie-wrap">
          <svg viewBox="0 0 240 240" className="pie-svg" role="img" aria-label={title}>
            <circle cx="120" cy="120" r="88" fill="rgba(255,255,255,0.04)" />
            {slices.map((slice) => (
              <path
                key={slice.name}
                d={slice.path}
                fill={slice.color}
                className={`pie-slice ${slice.isActive ? "active" : ""}`}
                onMouseEnter={() => setHovered(slice)}
                onMouseLeave={() => setHovered(null)}
                onClick={() => onSliceSelect({ type: filterType, value: slice.name })}
              >
                <title>
                  {slice.name}: {formatCurrency(slice.value)} ({formatPercent(slice.weight_percent)})
                </title>
              </path>
            ))}
          </svg>

          <div className="pie-center">
            <span>{focusItem.name}</span>
            <strong>{formatPercent(focusItem.weight_percent)}</strong>
            <small>{formatCurrency(focusItem.value)}</small>
          </div>
        </div>

        <div className="pie-legend">
          {slices.map((slice) => (
            <button
              key={slice.name}
              type="button"
              className={`legend-item ${slice.isActive ? "active" : ""}`}
              onMouseEnter={() => setHovered(slice)}
              onMouseLeave={() => setHovered(null)}
              onClick={() => onSliceSelect({ type: filterType, value: slice.name })}
            >
              <span className="legend-dot" style={{ backgroundColor: slice.color }} />
              <span className="legend-label">{slice.name}</span>
              <span className="legend-value">{formatPercent(slice.weight_percent)}</span>
            </button>
          ))}
        </div>
      </div>
    </article>
  )
}

function buildLinePath(points, width, height, padding, key, minValue, maxValue) {
  const innerWidth = width - padding.left - padding.right
  const innerHeight = height - padding.top - padding.bottom
  const range = Math.max(maxValue - minValue, 1)

  return points
    .map((point, index) => {
      const x =
        padding.left +
        (points.length === 1 ? innerWidth / 2 : (index / (points.length - 1)) * innerWidth)
      const y = padding.top + ((maxValue - point[key]) / range) * innerHeight
      return `${index === 0 ? "M" : "L"} ${x} ${y}`
    })
    .join(" ")
}

function PerformanceChart({ comparison, benchmarkName, overview, benchmark }) {
  let points = comparison?.points ?? []
  let startDate = comparison?.start_date
  let endDate = comparison?.end_date

  if (points.length < 2) {
    const portfolioCurrent = Number(overview?.total_net_worth ?? 0)
    const portfolioOneDayChange = Number(overview?.one_day_change ?? 0)
    const portfolioPrevious = portfolioCurrent - portfolioOneDayChange
    const benchmarkPrice = Number(benchmark?.price ?? 0)
    const benchmarkPrevClose = Number(benchmark?.prev_close ?? 0)

    if (portfolioCurrent > 0 && portfolioPrevious > 0 && benchmarkPrice > 0 && benchmarkPrevClose > 0) {
      const today = todayInputValue()
      const yesterday = new Date()
      yesterday.setDate(yesterday.getDate() - 1)
      const previousDate = yesterday.toISOString().slice(0, 10)
      points = [
        {
          date: previousDate,
          portfolio_value: 100,
          benchmark_value: 100,
          portfolio_change_percent: 0,
          benchmark_change_percent: 0
        },
        {
          date: today,
          portfolio_value: (portfolioCurrent / portfolioPrevious) * 100,
          benchmark_value: (benchmarkPrice / benchmarkPrevClose) * 100,
          portfolio_change_percent: ((portfolioCurrent / portfolioPrevious) * 100) - 100,
          benchmark_change_percent: ((benchmarkPrice / benchmarkPrevClose) * 100) - 100
        }
      ]
      startDate = previousDate
      endDate = today
    }
  }

  if (points.length < 2) {
    return (
      <section className="panel">
        <div className="panel-heading">
          <div>
            <p className="panel-kicker">Performance</p>
            <h3>Portfolio vs benchmark</h3>
          </div>
        </div>
        <p className="empty-state">Refresh holdings on multiple days to build the normalized performance chart.</p>
      </section>
    )
  }

  const width = 760
  const height = 320
  const padding = { top: 20, right: 22, bottom: 42, left: 92 }
  const values = points.flatMap((point) => [point.portfolio_value, point.benchmark_value])
  const minValue = Math.min(...values)
  const maxValue = Math.max(...values)
  const startPoint = points[0]
  const endPoint = points[points.length - 1]
  const portfolioPath = buildLinePath(points, width, height, padding, "portfolio_value", minValue, maxValue)
  const benchmarkPath = buildLinePath(points, width, height, padding, "benchmark_value", minValue, maxValue)
  const innerHeight = height - padding.top - padding.bottom
  const yTicks = [0, 1, 2, 3].map((step) => {
    const value = maxValue - ((maxValue - minValue) / 3) * step
    const y = padding.top + (innerHeight / 3) * step
    return { value, y }
  })

  return (
    <section className="panel">
      <div className="panel-heading">
        <div>
          <p className="panel-kicker">Performance</p>
          <h3>Portfolio vs benchmark</h3>
        </div>
        <span>
          {startDate} to {endDate}
        </span>
      </div>

      <div className="performance-split">
        <div className="chart-summary">
          <div className="chart-summary-item">
            <span>Portfolio</span>
            <strong className={statusTone(endPoint.portfolio_change_percent)}>
              {formatPercent(endPoint.portfolio_change_percent)}
            </strong>
          </div>
          <div className="chart-summary-item">
            <span>{benchmarkName}</span>
            <strong className={statusTone(endPoint.benchmark_change_percent)}>
              {formatPercent(endPoint.benchmark_change_percent)}
            </strong>
          </div>
          <div className="chart-summary-item">
            <span>Spread</span>
            <strong
              className={statusTone(
                endPoint.portfolio_change_percent - endPoint.benchmark_change_percent
              )}
            >
              {formatPercent(
                endPoint.portfolio_change_percent - endPoint.benchmark_change_percent
              )}
            </strong>
          </div>
          <div className="chart-summary-item">
            <span>Normalized base</span>
            <strong>{formatNumber(startPoint.portfolio_value, 0)}</strong>
          </div>
        </div>

        <div>
          <div className="performance-chart-wrap">
            <svg viewBox={`0 0 ${width} ${height}`} className="performance-chart" role="img" aria-label="Portfolio vs benchmark performance">
              <line x1={padding.left} y1={padding.top} x2={padding.left} y2={height - padding.bottom} className="chart-axis-line" />
              <line x1={padding.left} y1={height - padding.bottom} x2={width - padding.right} y2={height - padding.bottom} className="chart-axis-line" />
              {yTicks.map((tick, index) => {
                return (
                  <g key={index}>
                    <line x1={padding.left} y1={tick.y} x2={width - padding.right} y2={tick.y} className="chart-grid-line" />
                    <text x={padding.left - 12} y={tick.y + 4} textAnchor="end" className="chart-axis-text">
                      {formatNumber(tick.value, 0)}
                    </text>
                  </g>
                )
              })}
              <text x={28} y={height / 2} transform={`rotate(-90 28 ${height / 2})`} className="chart-axis-label">
                Normalized value (Base = 100)
              </text>
              <text x={width / 2} y={height - 8} textAnchor="middle" className="chart-axis-label">
                Time
              </text>
              <text x={padding.left} y={height - 18} textAnchor="start" className="chart-axis-text">
                {formatDateLabel(startDate)}
              </text>
              <text x={width - padding.right} y={height - 18} textAnchor="end" className="chart-axis-text">
                {formatDateLabel(endDate)}
              </text>
              <path d={benchmarkPath} className="chart-line benchmark-line" />
              <path d={portfolioPath} className="chart-line portfolio-line" />
            </svg>
          </div>

          <div className="chart-legend">
            <span className="chart-legend-item">
              <span className="chart-swatch portfolio-line-swatch" />
              Portfolio
            </span>
            <span className="chart-legend-item">
              <span className="chart-swatch benchmark-line-swatch" />
              {benchmarkName}
            </span>
          </div>
        </div>
      </div>
    </section>
  )
}

function MiniTrendChart({ chart }) {
  const points = chart?.points ?? []

  if (points.length < 2) {
    return null
  }

  const width = 150
  const height = 44
  const padding = { top: 6, right: 4, bottom: 6, left: 4 }
  const values = points.map((point) => point.value)
  const minValue = Math.min(...values)
  const maxValue = Math.max(...values)
  const rangeMin = Math.min(minValue, Number(chart.prev_close ?? minValue))
  const rangeMax = Math.max(maxValue, Number(chart.prev_close ?? maxValue))
  const path = buildLinePath(points, width, height, padding, "value", rangeMin, rangeMax)
  const innerHeight = height - padding.top - padding.bottom
  const baselineY =
    chart.prev_close !== null && chart.prev_close !== undefined
      ? padding.top + ((rangeMax - chart.prev_close) / Math.max(rangeMax - rangeMin, 1)) * innerHeight
      : null

  return (
    <article className={`mini-chart-card ${chart.trend || ""}`}>
      <div className="mini-chart-head">
        <div>
          <span>{chart.name}</span>
          <strong>{chart.current_level !== null && chart.current_level !== undefined ? formatNumber(chart.current_level, 2) : "N/A"}</strong>
        </div>
        <div className="mini-chart-meta">
          <small>{chart.symbol}</small>
          <strong className={statusTone(chart.points_change ?? 0)}>
            {chart.points_change !== null && chart.points_change !== undefined
              ? `${chart.points_change >= 0 ? "+" : ""}${formatNumber(chart.points_change, 2)}`
              : "N/A"}
          </strong>
          <span className={statusTone(chart.change_percent ?? 0)}>
            {chart.change_percent !== null && chart.change_percent !== undefined
              ? formatPercent(chart.change_percent)
              : "N/A"}
          </span>
        </div>
      </div>
      <svg viewBox={`0 0 ${width} ${height}`} className="mini-chart" role="img" aria-label={`${chart.name} trend`}>
        {baselineY !== null ? (
          <line x1={padding.left} y1={baselineY} x2={width - padding.right} y2={baselineY} className="mini-chart-baseline" />
        ) : null}
        <path d={path} className={`mini-chart-line ${chart.trend || ""}`} />
      </svg>
    </article>
  )
}

function TransactionModal({
  holding,
  form,
  onChange,
  onClose,
  onSubmit,
  submitting
}) {
  if (!holding) {
    return null
  }

  return (
    <div className="modal-backdrop" role="presentation" onClick={onClose}>
      <div
        className="modal-card"
        role="dialog"
        aria-modal="true"
        aria-label={`Update ${holding.symbol}`}
        onClick={(event) => event.stopPropagation()}
      >
        <div className="panel-heading modal-heading">
          <div>
            <p className="panel-kicker">Manual update</p>
            <h3>{holding.company_name || holding.symbol}</h3>
          </div>
          <button className="ghost-button" type="button" onClick={onClose}>
            Close
          </button>
        </div>

        <form className="transaction-form" onSubmit={onSubmit}>
          <label>
            <span>Transaction type</span>
            <select name="type" value={form.type} onChange={onChange}>
              <option value="BUY">Buy</option>
              <option value="SELL">Sell</option>
            </select>
          </label>
          <label>
            <span>Quantity</span>
            <input name="quantity" type="number" min="0.0001" step="0.0001" value={form.quantity} onChange={onChange} required />
          </label>
          <label>
            <span>{form.type === "BUY" ? "Buy avg" : "Sell avg"}</span>
            <input name="price" type="number" min="0.01" step="0.01" value={form.price} onChange={onChange} required />
          </label>
          <label>
            <span>Transaction date</span>
            <input name="date" type="date" value={form.date} onChange={onChange} required />
          </label>

          <div className="modal-note">
            Current quantity: <strong>{formatNumber(holding.quantity, 4)}</strong> · Avg buy:{" "}
            <strong>{formatCurrency(holding.avg_buy_cost)}</strong>
          </div>

          <div className="modal-actions">
            <button className="ghost-button" type="button" onClick={onClose}>
              Cancel
            </button>
            <button className="secondary-button" type="submit" disabled={submitting}>
              {submitting ? "Saving..." : `${form.type === "BUY" ? "Buy" : "Sell"} stock`}
            </button>
          </div>
        </form>
      </div>
    </div>
  )
}

function SipModal({
  holding,
  form,
  onChange,
  onClose,
  onSubmit,
  submitting
}) {
  if (!holding) {
    return null
  }

  return (
    <div className="modal-backdrop" role="presentation" onClick={onClose}>
      <div
        className="modal-card"
        role="dialog"
        aria-modal="true"
        aria-label={`Create SIP for ${holding.symbol}`}
        onClick={(event) => event.stopPropagation()}
      >
        <div className="panel-heading modal-heading">
          <div>
            <p className="panel-kicker">Recurring SIP</p>
            <h3>{holding.company_name || holding.symbol}</h3>
          </div>
          <button className="ghost-button" type="button" onClick={onClose}>
            Close
          </button>
        </div>

        <form className="transaction-form" onSubmit={onSubmit}>
          <label>
            <span>Monthly amount</span>
            <input name="amount" type="number" min="1" step="1" value={form.amount} onChange={onChange} required />
          </label>
          <label>
            <span>First SIP date</span>
            <input name="start_date" type="date" value={form.start_date} onChange={onChange} required />
          </label>
          <div className="modal-note">
            The SIP will execute every month on the same calendar day using the latest available mutual fund price.
          </div>
          <div className="modal-actions">
            <button className="ghost-button" type="button" onClick={onClose}>
              Cancel
            </button>
            <button className="secondary-button" type="submit" disabled={submitting}>
              {submitting ? "Saving..." : "Create SIP"}
            </button>
          </div>
        </form>
      </div>
    </div>
  )
}

function App() {
  const [category, setCategory] = useState("ALL")
  const [dashboard, setDashboard] = useState(null)
  const [loading, setLoading] = useState(true)
  const [refreshing, setRefreshing] = useState(false)
  const [uploading, setUploading] = useState(false)
  const [submittingTransaction, setSubmittingTransaction] = useState(false)
  const [error, setError] = useState("")
  const [notice, setNotice] = useState("")
  const [allocationFilter, setAllocationFilter] = useState(null)
  const [activeActionSymbol, setActiveActionSymbol] = useState(null)
  const [transactionHolding, setTransactionHolding] = useState(null)
  const [sipHolding, setSipHolding] = useState(null)
  const [transactionForm, setTransactionForm] = useState({
    type: "BUY",
    quantity: "",
    price: "",
    date: todayInputValue()
  })
  const [sipForm, setSipForm] = useState({
    amount: "",
    start_date: todayInputValue()
  })

  const loadDashboard = useCallback(async (selectedCategory = category, showLoader = true) => {
    if (showLoader) {
      setLoading(true)
    } else {
      setRefreshing(true)
    }

    setError("")

    try {
      const data = await getImportedDashboard(selectedCategory)
      setDashboard(data)
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load imported portfolio")
    } finally {
      setLoading(false)
      setRefreshing(false)
    }
  }, [category])

  useEffect(() => {
    loadDashboard(category, true)
  }, [category, loadDashboard])

  async function handleUpload(event) {
    const file = event.target.files?.[0]
    if (!file) {
      return
    }

    if (!file.name.toLowerCase().endsWith(".xlsx")) {
      setError("Please upload a broker holdings file in .xlsx format.")
      return
    }

    setUploading(true)
    setError("")
    setNotice("")

    try {
      const content_base64 = await fileToBase64(file)
      const result = await importHoldingsWorkbook({
        filename: file.name,
        content_base64
      })
      setNotice(`${result.rows_imported ?? 0} holding rows imported from ${result.source_file}.`)
      setAllocationFilter(null)
      setCategory("ALL")
      await loadDashboard("ALL", false)
    } catch (err) {
      setError(err instanceof Error ? err.message : "Import failed")
    } finally {
      setUploading(false)
      event.target.value = ""
    }
  }

  async function handleRefresh() {
    setRefreshing(true)
    setError("")
    setNotice("")

    try {
      const result = await refreshImportedHoldings()
      setNotice(
        `Market data refreshed for ${result.updated_count ?? 0} holdings${
          result.failed_symbols?.length ? `, unavailable: ${result.failed_symbols.join(", ")}` : ""
        }.`
      )
      await loadDashboard(category, false)
    } catch (err) {
      setError(err instanceof Error ? err.message : "Refresh failed")
      setRefreshing(false)
    }
  }

  function openTransactionModal(holding, type) {
    setActiveActionSymbol(null)
    setTransactionHolding(holding)
    setTransactionForm({
      type,
      quantity: "",
      price: holding.avg_buy_cost ? String(holding.avg_buy_cost) : "",
      date: todayInputValue()
    })
  }

  function closeTransactionModal() {
    setTransactionHolding(null)
    setTransactionForm({
      type: "BUY",
      quantity: "",
      price: "",
      date: todayInputValue()
    })
  }

  function openSipModal(holding) {
    setActiveActionSymbol(null)
    setSipHolding(holding)
    setSipForm({
      amount: "",
      start_date: todayInputValue()
    })
  }

  function closeSipModal() {
    setSipHolding(null)
    setSipForm({
      amount: "",
      start_date: todayInputValue()
    })
  }

  function handleTransactionChange(event) {
    const { name, value } = event.target
    setTransactionForm((current) => ({
      ...current,
      [name]: value
    }))
  }

  function handleSipChange(event) {
    const { name, value } = event.target
    setSipForm((current) => ({
      ...current,
      [name]: value
    }))
  }

  async function handleTransactionSubmit(event) {
    event.preventDefault()
    if (!transactionHolding) {
      return
    }

    setSubmittingTransaction(true)
    setError("")
    setNotice("")

    try {
      const result = await applyImportedHoldingTransaction({
        symbol: transactionHolding.symbol,
        type: transactionForm.type,
        quantity: Number(transactionForm.quantity),
        price: Number(transactionForm.price),
        date: transactionForm.date
      })
      setNotice(result.message || "Holding updated successfully.")
      closeTransactionModal()
      await loadDashboard(category, false)
    } catch (err) {
      setError(err instanceof Error ? err.message : "Transaction update failed")
    } finally {
      setSubmittingTransaction(false)
    }
  }

  async function handleSipSubmit(event) {
    event.preventDefault()
    if (!sipHolding) {
      return
    }

    setSubmittingTransaction(true)
    setError("")
    setNotice("")

    try {
      const result = await addRecurringSip({
        symbol: sipHolding.symbol,
        amount: Number(sipForm.amount),
        start_date: sipForm.start_date
      })
      setNotice(
        `SIP created for ${result.symbol}: ${formatCurrency(result.amount)} every month from ${result.start_date}.`
      )
      closeSipModal()
      await loadDashboard(category, false)
    } catch (err) {
      setError(err instanceof Error ? err.message : "SIP creation failed")
    } finally {
      setSubmittingTransaction(false)
    }
  }

  const overview = dashboard?.overview
  const holdings = useMemo(() => dashboard?.holdings ?? [], [dashboard])
  const benchmark = dashboard?.benchmark
  const benchmarkCharts = dashboard?.benchmark_charts ?? []
  const riskMetrics = dashboard?.risk_metrics
  const performanceComparison = dashboard?.performance_comparison
  const recurringSips = dashboard?.recurring_sips ?? []
  const assetAllocation = dashboard?.asset_allocation ?? []
  const sectorAllocation = dashboard?.sector_allocation ?? []
  const categories = overview?.available_categories ?? ["ALL"]

  const filteredHoldings = useMemo(() => {
    if (!allocationFilter) {
      return holdings
    }

    if (allocationFilter.type === "asset") {
      return holdings.filter((holding) => holding.asset_type === allocationFilter.value)
    }

    if (allocationFilter.type === "sector") {
      return holdings.filter((holding) => (holding.sector || "Unclassified") === allocationFilter.value)
    }

    return holdings
  }, [allocationFilter, holdings])

  const filterLabel = allocationFilter
    ? `${allocationFilter.type === "asset" ? "Asset" : "Sector"}: ${allocationFilter.value}`
    : null

  return (
    <div className="app-shell">
      <section className="hero">
        <div>
          <p className="eyebrow">Automated Portfolio Tracking</p>
          <h1>Upload broker holdings. Refresh market data. Read the portfolio in one glance.</h1>
          <p className="hero-copy">
            This dashboard is centered on broker-file imports instead of manual trade entry.
            It computes live net worth, one-day movement, allocations, stock-level details,
            and Nifty 50 comparison from the imported snapshot.
          </p>
        </div>

        <div className="hero-actions">
          <label className="upload-card">
            <span>Import broker holdings (.xlsx)</span>
            <input type="file" accept=".xlsx" onChange={handleUpload} disabled={uploading} />
            <strong>{uploading ? "Importing..." : "Choose file"}</strong>
          </label>

          <button className="secondary-button" onClick={handleRefresh} disabled={refreshing || !dashboard}>
            {refreshing ? "Refreshing..." : "Refresh latest prices"}
          </button>

          <div className="hero-meta">
            <span>Latest import</span>
            <strong>{dashboard?.import_file_name || "No file imported yet"}</strong>
            <span>{dashboard?.imported_at ? formatDateTime(dashboard.imported_at) : "Upload a holdings workbook to begin."}</span>
          </div>
        </div>
      </section>

      {error ? <div className="banner error-banner">{error}</div> : null}
      {notice ? <div className="banner success-banner">{notice}</div> : null}

      {loading ? (
        <div className="loading-panel">Loading imported portfolio...</div>
      ) : !dashboard ? (
        <section className="panel empty-panel">
          <p className="panel-kicker">Ready for import</p>
          <h3>Upload a broker holdings `.xlsx` file to build the portfolio snapshot.</h3>
          <p className="empty-state">
            Once imported, the app will calculate total net worth, total gain, one-day change,
            allocations, Nifty 50 comparison, and stock-level analytics.
          </p>
        </section>
      ) : (
        <>
          <section className="toolbar">
            <div className="chip-group">
              {categories.map((option) => {
                const value = option.toUpperCase().replace(/\s+/g, "_")
                const active = value === category
                return (
                  <button
                    key={option}
                    className={`chip ${active ? "active" : ""}`}
                    onClick={() => {
                      setAllocationFilter(null)
                      setCategory(value)
                    }}
                    type="button"
                  >
                    {option === "ALL" ? "All Assets" : option}
                  </button>
                )
              })}
            </div>
            <p className="toolbar-note">
              Viewing: <strong>{overview.selected_category}</strong> · {overview.holdings_count} holdings
              {recurringSips.length ? ` · ${recurringSips.length} SIP${recurringSips.length > 1 ? "s" : ""} active` : ""}
            </p>
          </section>

          <section className="summary-grid">
            <article className="summary-card accent">
              <p>Total Net Worth</p>
              <h2>{formatCurrency(overview.total_net_worth)}</h2>
              <span>Imported holdings market value</span>
            </article>
            <article className={`summary-card ${statusTone(overview.total_gain)}`}>
              <p>Total Gain</p>
              <h2>{formatCurrency(overview.total_gain)}</h2>
              <span>{formatPercent(overview.total_gain_percent)}</span>
            </article>
            <article className={`summary-card ${statusTone(overview.one_day_change)}`}>
              <p>1D Change</p>
              <h2>{formatCurrency(overview.one_day_change)}</h2>
              <span>{formatPercent(overview.one_day_change_percent)}</span>
            </article>
            <article className="summary-card neutral">
              <p>Portfolio Avg P/E</p>
              <h2>{dashboard.portfolio_avg_pe !== null ? formatNumber(dashboard.portfolio_avg_pe, 2) : "N/A"}</h2>
              <span>vs Nifty 50: {dashboard.benchmark_pe_gap !== null ? formatNumber(dashboard.benchmark_pe_gap, 2) : "N/A"}</span>
            </article>
          </section>

          <section className="panel benchmark-panel">
            <div className="panel-heading">
              <div>
                <p className="panel-kicker">Benchmark</p>
                <h3>Nifty 50 comparison</h3>
              </div>
              <span>{benchmark?.symbol || "^NSEI"}</span>
            </div>

            <div className="benchmark-layout">
              <div className="benchmark-metrics">
                <div className="benchmark-grid">
                  <div className="metric-card benchmark-card">
                    <span>1D move</span>
                    <strong className={statusTone(benchmark?.one_day_change_percent ?? 0)}>
                      {benchmark?.one_day_change_percent !== null ? formatPercent(benchmark.one_day_change_percent) : "N/A"}
                    </strong>
                  </div>
                  <div className="metric-card benchmark-card">
                    <span>Index P/E</span>
                    <strong>{benchmark?.pe_ratio !== null ? formatNumber(benchmark.pe_ratio, 2) : "N/A"}</strong>
                  </div>
                  <div className="metric-card benchmark-card">
                    <span>Portfolio vs index 1D</span>
                    <strong className={statusTone((overview.one_day_change_percent ?? 0) - (benchmark?.one_day_change_percent ?? 0))}>
                      {benchmark?.one_day_change_percent !== null
                        ? formatPercent(overview.one_day_change_percent - benchmark.one_day_change_percent)
                        : "N/A"}
                    </strong>
                  </div>
                  <div className="metric-card benchmark-card">
                    <span>Sharpe ratio</span>
                    <strong>
                      {riskMetrics?.sharpe_ratio !== null && riskMetrics?.sharpe_ratio !== undefined
                        ? formatNumber(riskMetrics.sharpe_ratio, 2)
                        : "N/A"}
                    </strong>
                  </div>
                  <div className="metric-card benchmark-card">
                    <span>Beta</span>
                    <strong>
                      {riskMetrics?.beta !== null && riskMetrics?.beta !== undefined
                        ? formatNumber(riskMetrics.beta, 2)
                        : "N/A"}
                    </strong>
                  </div>
                  <div className="metric-card benchmark-card">
                    <span>Alpha</span>
                    <strong className={statusTone(riskMetrics?.alpha_annualized_percent ?? 0)}>
                      {riskMetrics?.alpha_annualized_percent !== null && riskMetrics?.alpha_annualized_percent !== undefined
                        ? formatPercent(riskMetrics.alpha_annualized_percent)
                        : "N/A"}
                    </strong>
                  </div>
                </div>
              </div>
              <div className="benchmark-empty">
                {benchmarkCharts.map((chart) => (
                  <MiniTrendChart key={chart.symbol} chart={chart} />
                ))}
              </div>
            </div>
          </section>

          <PerformanceChart
            comparison={performanceComparison}
            benchmarkName={benchmark?.name || benchmark?.symbol || "Benchmark"}
            overview={overview}
            benchmark={benchmark}
          />

          <section className="panel-grid">
            <AllocationPie
              title="Asset weightage"
              subtitle="Allocation"
              items={assetAllocation}
              filterType="asset"
              activeFilter={allocationFilter}
              onSliceSelect={setAllocationFilter}
            />
            <AllocationPie
              title="Sector allocation"
              subtitle="Diversification"
              items={sectorAllocation}
              filterType="sector"
              activeFilter={allocationFilter}
              onSliceSelect={setAllocationFilter}
            />
          </section>

          <section className="panel">
            <div className="panel-heading">
              <div>
                <p className="panel-kicker">Stock details</p>
                <h3>Holdings breakdown</h3>
              </div>
              <div className="table-actions">
                {filterLabel ? <span className="filter-badge">{filterLabel}</span> : null}
                {allocationFilter ? (
                  <button className="ghost-button" type="button" onClick={() => setAllocationFilter(null)}>
                    Back to all holdings
                  </button>
                ) : (
                  <span>Weight, pricing, sector, and unrealized performance</span>
                )}
              </div>
            </div>

            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Company</th>
                    <th>Type</th>
                    <th>Sector</th>
                    <th>Weight</th>
                    <th>Avg buy cost</th>
                    <th>Investment</th>
                    <th>Current value</th>
                    <th>1D change</th>
                    <th>Unrealised P&amp;L</th>
                    <th>P/E</th>
                    <th />
                  </tr>
                </thead>
                <tbody>
                  {filteredHoldings.length ? (
                    filteredHoldings.map((holding) => (
                      <tr key={`${holding.symbol}-${holding.asset_type}`}>
                        <td>
                          <div className="company-cell">
                            <strong>{holding.company_name || holding.symbol}</strong>
                            <span>{holding.symbol}</span>
                          </div>
                        </td>
                        <td>{holding.asset_type}</td>
                        <td>{holding.sector || "Unclassified"}</td>
                        <td>{formatPercent(holding.weight_percent)}</td>
                        <td>{formatCurrency(holding.avg_buy_cost)}</td>
                        <td>{formatCurrency(holding.invested_amount)}</td>
                        <td>{formatCurrency(holding.current_value)}</td>
                        <td className={statusTone(holding.one_day_change)}>{formatCurrency(holding.one_day_change)}</td>
                        <td className={statusTone(holding.unrealized_pnl)}>{formatCurrency(holding.unrealized_pnl)}</td>
                        <td>{holding.pe_ratio !== null ? formatNumber(holding.pe_ratio, 2) : "N/A"}</td>
                        <td className="row-action-cell">
                          <div className="row-action-wrap">
                            <button
                              className="row-action-button"
                              type="button"
                              aria-label={`Actions for ${holding.symbol}`}
                              onClick={() =>
                                setActiveActionSymbol((current) =>
                                  current === holding.symbol ? null : holding.symbol
                                )
                              }
                            >
                              ...
                            </button>
                            {activeActionSymbol === holding.symbol ? (
                              <div className="row-action-menu">
                                <button type="button" onClick={() => openTransactionModal(holding, "BUY")}>
                                  Buy more
                                </button>
                                <button type="button" onClick={() => openTransactionModal(holding, "SELL")}>
                                  Sell
                                </button>
                                {holding.asset_type === "Mutual Fund" ? (
                                  <button type="button" onClick={() => openSipModal(holding)}>
                                    Create SIP
                                  </button>
                                ) : null}
                              </div>
                            ) : null}
                          </div>
                        </td>
                      </tr>
                    ))
                  ) : (
                    <tr>
                      <td colSpan="11" className="empty-state">
                        No holdings match the selected filter.
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </section>
        </>
      )}

      <TransactionModal
        holding={transactionHolding}
        form={transactionForm}
        onChange={handleTransactionChange}
        onClose={closeTransactionModal}
        onSubmit={handleTransactionSubmit}
        submitting={submittingTransaction}
      />
      <SipModal
        holding={sipHolding}
        form={sipForm}
        onChange={handleSipChange}
        onClose={closeSipModal}
        onSubmit={handleSipSubmit}
        submitting={submittingTransaction}
      />
    </div>
  )
}

export default App
