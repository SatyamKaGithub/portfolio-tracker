import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import {
  applyImportedHoldingTransaction,
  addRecurringSip,
  clearStoredSession,
  getCurrentUser,
  getImportedDashboard,
  getNifty50Snapshot,
  login,
  importHoldingsWorkbook,
  logout,
  refreshImportedHoldings,
  getSipJobStatus,
  runSipJob,
  signup
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

const MARKET_NEWS_ITEMS = [
  {
    id: "news-1",
    headline: "Banking stocks lead gains as credit growth expectations improve",
    highlight: "Private banks outperformed with strong volume signals in the last session.",
    keyDetail: "Portfolio watch: financial allocation saw the strongest intraday contribution.",
    source: "Market Pulse",
    publishedAt: "2h ago",
    url: "https://example.com/market-news/banking-gains"
  },
  {
    id: "news-2",
    headline: "IT names remain range-bound ahead of US macro data",
    highlight: "Large-cap IT traded in a narrow band with mixed momentum readings.",
    keyDetail: "Portfolio watch: near-term volatility expected in export-driven holdings.",
    source: "Business Track",
    publishedAt: "3h ago",
    url: "https://example.com/market-news/it-range-bound"
  },
  {
    id: "news-3",
    headline: "Auto counters gain on steady monthly dispatch trends",
    highlight: "Passenger and two-wheeler names saw broad-based buying.",
    keyDetail: "Portfolio watch: cyclical sectors continue to support overall P&L.",
    source: "Street Brief",
    publishedAt: "4h ago",
    url: "https://example.com/market-news/auto-dispatch"
  },
  {
    id: "news-4",
    headline: "Mid and small caps witness selective profit booking",
    highlight: "High-beta names cooled off after a multi-session rally.",
    keyDetail: "Portfolio watch: rebalance candidates may appear in overheated pockets.",
    source: "Capital Wire",
    publishedAt: "5h ago",
    url: "https://example.com/market-news/midcap-booking"
  },
  {
    id: "news-5",
    headline: "Defensive FMCG stocks draw fresh institutional interest",
    highlight: "Investors rotated toward lower-volatility consumption names.",
    keyDetail: "Portfolio watch: defensives helped offset intraday risk-off moves.",
    source: "Daily Markets",
    publishedAt: "6h ago",
    url: "https://example.com/market-news/fmcg-defensive"
  },
  {
    id: "news-6",
    headline: "Energy complex stable despite crude price swings",
    highlight: "Integrated players held key support levels through the session.",
    keyDetail: "Portfolio watch: margin outlook remains sensitive to input-price changes.",
    source: "FinScope",
    publishedAt: "7h ago",
    url: "https://example.com/market-news/energy-stable"
  },
  {
    id: "news-7",
    headline: "Mutual fund SIP trends remain resilient month-on-month",
    highlight: "Steady retail inflows continue to support long-duration allocations.",
    keyDetail: "Portfolio watch: long-term compounding themes remain intact.",
    source: "Investor Desk",
    publishedAt: "8h ago",
    url: "https://example.com/market-news/sip-trends"
  }
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

function formatSignedNumber(value, digits = 2) {
  const amount = Number(value)
  if (!Number.isFinite(amount)) {
    return "N/A"
  }

  return `${amount >= 0 ? "+" : ""}${formatNumber(amount, digits)}`
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

function formatDate(value) {
  if (!value) {
    return "N/A"
  }

  const date = new Date(value)
  if (Number.isNaN(date.getTime())) {
    return value
  }

  return new Intl.DateTimeFormat("en-IN", {
    dateStyle: "medium"
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

function formatMarketStatus(value) {
  const date = value ? new Date(value) : new Date()

  if (Number.isNaN(date.getTime())) {
    return "Markets Closed"
  }

  const dateLabel = new Intl.DateTimeFormat("en-IN", {
    day: "2-digit",
    month: "short",
    year: "numeric",
    timeZone: "Asia/Kolkata"
  }).format(date)

  return `Markets Closed • ${dateLabel} at IST`
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

function buildTreemapLayout(items, x, y, width, height, depth = 0) {
  if (!items.length || width <= 0 || height <= 0) {
    return []
  }

  if (items.length === 1) {
    return [
      {
        ...items[0],
        x,
        y,
        width,
        height
      }
    ]
  }

  const total = items.reduce((sum, item) => sum + Number(item.value || 0), 0)
  if (total <= 0) {
    return []
  }

  const isVerticalSplit = (depth % 2 === 0 ? width >= height : width < height)
  let splitIndex = 0
  let firstGroupValue = 0

  while (splitIndex < items.length - 1 && firstGroupValue < total / 2) {
    firstGroupValue += Number(items[splitIndex].value || 0)
    splitIndex += 1
  }

  const firstItems = items.slice(0, splitIndex)
  const secondItems = items.slice(splitIndex)
  const firstRatio = Math.max(0.08, Math.min(firstGroupValue / total, 0.92))

  if (isVerticalSplit) {
    const firstWidth = width * firstRatio
    return [
      ...buildTreemapLayout(firstItems, x, y, firstWidth, height, depth + 1),
      ...buildTreemapLayout(secondItems, x + firstWidth, y, width - firstWidth, height, depth + 1)
    ]
  }

  const firstHeight = height * firstRatio
  return [
    ...buildTreemapLayout(firstItems, x, y, width, firstHeight, depth + 1),
    ...buildTreemapLayout(secondItems, x, y + firstHeight, width, height - firstHeight, depth + 1)
  ]
}

function SectorTreemap({ title, subtitle, items, activeFilter, onSectorSelect }) {
  const [hovered, setHovered] = useState(null)
  const treemapRef = useRef(null)
  const totalCurrentValue = items.reduce((sum, item) => sum + Number(item.currentValue || 0), 0)

  if (!items.length || totalCurrentValue <= 0) {
    return (
      <article className="panel">
        <div className="panel-heading">
          <div>
            <p className="panel-kicker">{subtitle}</p>
            <h3>{title}</h3>
          </div>
        </div>
        <p className="empty-state">No sector allocation data yet.</p>
      </article>
    )
  }

  const sortedItems = [...items].sort((a, b) => Number(b.value || 0) - Number(a.value || 0))
  const treemap = buildTreemapLayout(sortedItems, 0, 0, 100, 100)

  return (
    <article className="panel sector-treemap-panel">
      <div className="panel-heading">
        <div>
          <p className="panel-kicker">{subtitle}</p>
          <h3>{title}</h3>
        </div>
        {activeFilter?.type === "sector" ? (
          <button className="ghost-button" type="button" onClick={() => onSectorSelect(null)}>
            Back to all
          </button>
        ) : null}
      </div>

      <div className="treemap-wrap" ref={treemapRef}>
        {treemap.map((item) => {
          const isActive = activeFilter?.type === "sector" && activeFilter?.value === item.name
          const tone = Number(item.pnl) >= 0 ? "positive" : "negative"

          return (
            <button
              key={item.name}
              type="button"
              className={`treemap-node ${tone} ${isActive ? "active" : ""}`}
              style={{
                left: `${item.x}%`,
                top: `${item.y}%`,
                width: `${item.width}%`,
                height: `${item.height}%`
              }}
              onClick={() => onSectorSelect({ type: "sector", value: item.name })}
              onMouseEnter={(event) => {
                const wrapRect = treemapRef.current?.getBoundingClientRect()
                if (!wrapRect) {
                  setHovered({ item, x: 16, y: 16 })
                  return
                }
                const nextX = Math.max(
                  12,
                  Math.min(event.clientX - wrapRect.left + 10, wrapRect.width - 220)
                )
                const nextY = Math.max(
                  16,
                  Math.min(event.clientY - wrapRect.top + 10, wrapRect.height - 110)
                )
                setHovered({
                  item,
                  x: nextX,
                  y: nextY
                })
              }}
              onMouseMove={(event) => {
                const wrapRect = treemapRef.current?.getBoundingClientRect()
                if (!wrapRect) {
                  return
                }
                const nextX = Math.max(
                  12,
                  Math.min(event.clientX - wrapRect.left + 10, wrapRect.width - 220)
                )
                const nextY = Math.max(
                  16,
                  Math.min(event.clientY - wrapRect.top + 10, wrapRect.height - 110)
                )
                setHovered({
                  item,
                  x: nextX,
                  y: nextY
                })
              }}
              onMouseLeave={() => setHovered(null)}
            >
              <span>{item.name}</span>
            </button>
          )
        })}

        {hovered ? (
          <div
            className="treemap-tooltip"
            style={{
              left: `${hovered.x}px`,
              top: `${hovered.y}px`
            }}
          >
            <strong>{hovered.item.name}</strong>
            <span>Holding: {formatPercent(hovered.item.weightPercent)}</span>
            <span>Invested: {formatCurrency(hovered.item.invested)}</span>
            <span>Current: {formatCurrency(hovered.item.currentValue)}</span>
            <span className={statusTone(hovered.item.pnl)}>P&amp;L: {formatCurrency(hovered.item.pnl)}</span>
          </div>
        ) : null}
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

function PerformanceChart({
  comparison,
  benchmarkName,
  performancePeriod,
  onPeriodChange
}) {
  const points = comparison?.points ?? []
  const startDate = comparison?.start_date
  const endDate = comparison?.end_date

  if (points.length < 2) {
    return (
      <section className="panel">
        <div className="panel-heading">
          <div>
            <p className="panel-kicker">Performance</p>
            <h3>Portfolio vs benchmark</h3>
          </div>
          <div className="range-chip-group">
            {["1Y", "3Y", "5Y"].map((period) => (
              <button
                key={period}
                type="button"
                className={`chip ${performancePeriod === period ? "active" : ""}`}
                onClick={() => onPeriodChange(period)}
              >
                {period}
              </button>
            ))}
          </div>
        </div>
        <p className="empty-state">
          Not enough snapshot history for {performancePeriod} comparison. Refresh market data daily to build history.
        </p>
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
        <div className="range-controls">
          <div className="range-chip-group">
            {["1Y", "3Y", "5Y"].map((period) => (
              <button
                key={period}
                type="button"
                className={`chip ${performancePeriod === period ? "active" : ""}`}
                onClick={() => onPeriodChange(period)}
              >
                {period}
              </button>
            ))}
          </div>
          <span>
            {startDate} to {endDate}
          </span>
        </div>
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

function MiniTrendChart({ chart, compact = false }) {
  const points = chart?.points ?? []

  if (points.length < 2) {
    return null
  }

  const width = compact ? 120 : 150
  const height = compact ? 30 : 44
  const padding = compact
    ? { top: 4, right: 4, bottom: 4, left: 4 }
    : { top: 6, right: 4, bottom: 6, left: 4 }
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
    <article className={`mini-chart-card ${chart.trend || ""} ${compact ? "compact" : ""}`}>
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

function MarketTicker({ items }) {
  if (!items.length) {
    return null
  }

  const repeatedItems = [...items, ...items]

  return (
    <section className="market-ticker" aria-label="Nifty 50 daily movement">
      <div className="ticker-track">
        {repeatedItems.map((item, index) => (
          <div key={`${item.symbol}-${index}`} className="ticker-item">
            <span className="ticker-symbol">{item.name}</span>
            <strong className={statusTone(item.change)}>
              {formatSignedNumber(item.change)}%
            </strong>
          </div>
        ))}
      </div>
    </section>
  )
}

function MarketSentimentCard({ benchmark, overview }) {
  const niftyMove = Number(benchmark?.one_day_change_percent ?? 0)
  let score = 5
  let tone = "uncertain"
  let label = "Uncertain Sentiment"

  if (niftyMove >= 0.35) {
    tone = "bullish"
    label = "Bullish Sentiment"
    const momentum = Math.min((niftyMove - 0.35) / 0.85, 1)
    score = 8 + Math.round(momentum * 2)
  } else if (niftyMove >= 0.05) {
    tone = "bullish"
    label = "Bullish Sentiment"
    const momentum = Math.min((niftyMove - 0.05) / 0.3, 1)
    score = 6 + Math.round(momentum * 2)
  } else if (niftyMove > -0.05) {
    tone = "uncertain"
    label = "Uncertain Sentiment"
    const momentum = (niftyMove + 0.05) / 0.1
    score = 4 + Math.round(Math.max(0, Math.min(momentum, 1)) * 2)
  } else if (niftyMove > -0.35) {
    tone = "bearish"
    label = "Bearish Sentiment"
    const severity = Math.min(Math.abs(niftyMove + 0.05) / 0.3, 1)
    score = Math.max(2, 4 - Math.round(severity * 2))
  } else {
    tone = "bearish"
    label = "Bearish Sentiment"
    const severity = Math.max(0, Math.min(Math.abs(niftyMove + 0.35) / 0.85, 1))
    score = Math.max(1, 2 - Math.floor(severity))
  }

  return (
    <article className={`panel sentiment-card ${tone}`}>
      <div className="sentiment-meter" aria-hidden="true">
        {Array.from({ length: 10 }).map((_, index) => (
          <span
            key={index}
            className={`sentiment-bar ${index < score ? `active ${tone}` : ""}`}
          />
        ))}
      </div>
      <h3>{label}</h3>
      <p>{formatMarketStatus(overview?.as_of)}</p>
    </article>
  )
}

function MarketNewsPanel({ items }) {
  return (
    <section className="panel market-news-panel">
      <div className="panel-heading">
        <div>
          <p className="panel-kicker">Market News</p>
          <h3>Portfolio highlights</h3>
        </div>
      </div>

      <div className="news-scroll">
        {items.map((item) => (
          <article key={item.id} className="news-item">
            <h4>{item.headline}</h4>
            <p>{item.highlight}</p>
            <p className="news-key">{item.keyDetail}</p>
            <div className="news-meta">
              <span>
                {item.source} · {item.publishedAt}
              </span>
              <a href={item.url} target="_blank" rel="noreferrer">
                Read full
              </a>
            </div>
          </article>
        ))}
      </div>
    </section>
  )
}

function PriceAlertModal({ open, onClose, form, onChange }) {
  if (!open) {
    return null
  }

  return (
    <div className="modal-backdrop" role="presentation" onClick={onClose}>
      <div
        className="modal-card"
        role="dialog"
        aria-modal="true"
        aria-label="Create stock price alert"
        onClick={(event) => event.stopPropagation()}
      >
        <div className="panel-heading modal-heading">
          <div>
            <p className="panel-kicker">Price alert</p>
            <h3>Create stock alert</h3>
          </div>
          <button className="ghost-button" type="button" onClick={onClose}>
            Close
          </button>
        </div>

        <form className="transaction-form" onSubmit={(event) => event.preventDefault()}>
          <label>
            <span>Stock symbol</span>
            <input
              name="symbol"
              type="text"
              placeholder="e.g. RELIANCE"
              value={form.symbol}
              onChange={onChange}
            />
          </label>
          <label>
            <span>Target price (INR)</span>
            <input
              name="targetPrice"
              type="number"
              min="0"
              step="0.01"
              placeholder="e.g. 3050"
              value={form.targetPrice}
              onChange={onChange}
            />
          </label>
          <label>
            <span>Duration</span>
            <select name="duration" value={form.duration} onChange={onChange}>
              <option value="1_WEEK">1 week</option>
              <option value="1_MONTH">1 month</option>
              <option value="3_MONTHS">3 months</option>
              <option value="UNTIL_HIT">Until target hit</option>
            </select>
          </label>
          <label>
            <span>Alert channel</span>
            <select name="channel" value={form.channel} onChange={onChange}>
              <option value="IN_APP">In-app notification</option>
              <option value="EMAIL">Email notification</option>
              <option value="BOTH">Email + in-app</option>
            </select>
          </label>
          <div className="modal-note">
            Placeholder feature: we have added the UI only. Trigger logic, notification engine, and
            email delivery will be implemented in the next phase.
          </div>
          <div className="modal-actions">
            <button className="ghost-button" type="button" onClick={onClose}>
              Cancel
            </button>
            <button className="secondary-button" type="button" disabled>
              Save alert (coming soon)
            </button>
          </div>
        </form>
      </div>
    </div>
  )
}

function AuthModal({
  open,
  mode,
  form,
  onChange,
  onClose,
  onSwitchMode,
  onSubmit,
  submitting
}) {
  if (!open) {
    return null
  }

  const isLogin = mode === "login"

  return (
    <div className="modal-backdrop" role="presentation" onClick={onClose}>
      <div
        className="modal-card"
        role="dialog"
        aria-modal="true"
        aria-label={isLogin ? "Login" : "Signup"}
        onClick={(event) => event.stopPropagation()}
      >
        <div className="panel-heading modal-heading">
          <div>
            <p className="panel-kicker">{isLogin ? "Welcome back" : "Create account"}</p>
            <h3>{isLogin ? "Login" : "Signup"}</h3>
          </div>
          <button className="ghost-button" type="button" onClick={onClose}>
            Close
          </button>
        </div>

        <form className="transaction-form" onSubmit={onSubmit}>
          {isLogin ? (
            <label>
              <span>Username or email</span>
              <input
                name="login"
                type="text"
                value={form.login}
                onChange={onChange}
                required
              />
            </label>
          ) : (
            <>
              <label>
                <span>Username</span>
                <input
                  name="username"
                  type="text"
                  value={form.username}
                  onChange={onChange}
                  required
                />
              </label>
              <label>
                <span>Email</span>
                <input
                  name="email"
                  type="email"
                  value={form.email}
                  onChange={onChange}
                  required
                />
              </label>
            </>
          )}
          <label>
            <span>Password</span>
            <input
              name="password"
              type="password"
              value={form.password}
              onChange={onChange}
              required
              minLength={8}
            />
          </label>
          {!isLogin ? (
            <label>
              <span>Confirm password</span>
              <input
                name="confirmPassword"
                type="password"
                value={form.confirmPassword}
                onChange={onChange}
                required
                minLength={8}
              />
            </label>
          ) : null}

          <div className="modal-actions">
            <button className="ghost-button" type="button" onClick={onSwitchMode}>
              {isLogin ? "Need an account? Signup" : "Have an account? Login"}
            </button>
            <button className="secondary-button" type="submit" disabled={submitting}>
              {submitting ? "Please wait..." : isLogin ? "Login" : "Signup"}
            </button>
          </div>
        </form>
      </div>
    </div>
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
  const [submittingAuth, setSubmittingAuth] = useState(false)
  const [runningSipJob, setRunningSipJob] = useState(false)
  const [error, setError] = useState("")
  const [notice, setNotice] = useState("")
  const [sipJobStatus, setSipJobStatus] = useState(null)
  const [niftyTickerRows, setNiftyTickerRows] = useState([])
  const [allocationFilter, setAllocationFilter] = useState(null)
  const [activeActionSymbol, setActiveActionSymbol] = useState(null)
  const [transactionHolding, setTransactionHolding] = useState(null)
  const [sipHolding, setSipHolding] = useState(null)
  const [authModalOpen, setAuthModalOpen] = useState(false)
  const [authMode, setAuthMode] = useState("login")
  const [currentUser, setCurrentUser] = useState(null)
  const [authForm, setAuthForm] = useState({
    username: "",
    email: "",
    login: "",
    password: "",
    confirmPassword: ""
  })
  const [searchTerm, setSearchTerm] = useState("")
  const [accountMenuOpen, setAccountMenuOpen] = useState(false)
  const [themeMode, setThemeMode] = useState("dark")
  const [performancePeriod, setPerformancePeriod] = useState("1Y")
  const [priceAlertOpen, setPriceAlertOpen] = useState(false)
  const [priceAlertForm, setPriceAlertForm] = useState({
    symbol: "",
    targetPrice: "",
    duration: "1_MONTH",
    channel: "IN_APP"
  })
  const accountMenuRef = useRef(null)
  const uploadInputRef = useRef(null)
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

  const loadNiftyTicker = useCallback(async () => {
    try {
      const data = await getNifty50Snapshot()
      setNiftyTickerRows(Array.isArray(data?.rows) ? data.rows : [])
    } catch {
      setNiftyTickerRows([])
    }
  }, [])

  const bootstrapSession = useCallback(async () => {
    try {
      const user = await getCurrentUser()
      setCurrentUser(user)
    } catch {
      clearStoredSession()
      setCurrentUser(null)
    }
  }, [])

  const loadSipJobOperationsStatus = useCallback(async (silent = true) => {
    try {
      const status = await getSipJobStatus()
      setSipJobStatus(status)
    } catch (err) {
      if (!silent) {
        setError(err instanceof Error ? err.message : "Failed to load SIP job status")
      }
    }
  }, [])

  const loadDashboard = useCallback(async (
    selectedCategory = category,
    showLoader = true,
    selectedPerformancePeriod = performancePeriod
  ) => {
    if (showLoader) {
      setLoading(true)
    } else {
      setRefreshing(true)
    }

    setError("")

    try {
      const data = await getImportedDashboard(selectedCategory, selectedPerformancePeriod)
      setDashboard(data)
      await loadSipJobOperationsStatus(true)
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load imported portfolio")
    } finally {
      setLoading(false)
      setRefreshing(false)
    }
  }, [category, loadSipJobOperationsStatus, performancePeriod])

  useEffect(() => {
    if (!currentUser) {
      setDashboard(null)
      setLoading(false)
      setRefreshing(false)
      return
    }
    loadDashboard(category, true, performancePeriod)
  }, [category, currentUser, performancePeriod, loadDashboard])

  useEffect(() => {
    if (!currentUser) {
      setSipJobStatus(null)
      return
    }
    loadSipJobOperationsStatus(true)
  }, [currentUser, loadSipJobOperationsStatus])

  useEffect(() => {
    bootstrapSession()
    loadNiftyTicker()
  }, [bootstrapSession, loadNiftyTicker])

  useEffect(() => {
    document.body.dataset.theme = themeMode
    return () => {
      delete document.body.dataset.theme
    }
  }, [themeMode])

  useEffect(() => {
    if (!accountMenuOpen) {
      return
    }

    function handleClickOutside(event) {
      if (accountMenuRef.current && !accountMenuRef.current.contains(event.target)) {
        setAccountMenuOpen(false)
      }
    }

    document.addEventListener("mousedown", handleClickOutside)
    return () => {
      document.removeEventListener("mousedown", handleClickOutside)
    }
  }, [accountMenuOpen])

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

  async function handleRunSipJob(force = false) {
    setRunningSipJob(true)
    setError("")
    setNotice("")

    try {
      const result = await runSipJob(force)
      if (result.status === "skipped") {
        setNotice("SIP job was already completed today. Use force rerun to run again.")
      } else {
        setNotice(`SIP job completed. Processed ${result.processed_sips ?? 0} SIP transaction(s).`)
      }
      await loadDashboard(category, false)
      await loadSipJobOperationsStatus(true)
    } catch (err) {
      setError(err instanceof Error ? err.message : "SIP job trigger failed")
    } finally {
      setRunningSipJob(false)
    }
  }

  const overview = dashboard?.overview
  const holdings = useMemo(() => dashboard?.holdings ?? [], [dashboard])
  const benchmark = dashboard?.benchmark
  const benchmarkCharts = dashboard?.benchmark_charts ?? []
  const riskMetrics = dashboard?.risk_metrics
  const performanceComparison = dashboard?.performance_comparison
  const recurringSips = useMemo(() => dashboard?.recurring_sips ?? [], [dashboard])
  const assetAllocation = dashboard?.asset_allocation ?? []
  const sectorAllocation = useMemo(() => dashboard?.sector_allocation ?? [], [dashboard])
  const categories = overview?.available_categories ?? ["ALL"]
  const sipScheduler = sipJobStatus?.scheduler
  const sipTotals = sipJobStatus?.totals
  const sipLastRun = sipJobStatus?.last_run

  const filteredHoldings = useMemo(() => {
    let nextHoldings = holdings

    if (allocationFilter?.type === "asset") {
      nextHoldings = nextHoldings.filter((holding) => holding.asset_type === allocationFilter.value)
    } else if (allocationFilter?.type === "sector") {
      nextHoldings = nextHoldings.filter(
        (holding) => (holding.sector || "Unclassified") === allocationFilter.value
      )
    }

    const query = searchTerm.trim().toLowerCase()
    if (!query) {
      return nextHoldings
    }

    return nextHoldings.filter((holding) => {
      const haystack = [
        holding.company_name,
        holding.symbol,
        holding.sector,
        holding.asset_type
      ]
        .filter(Boolean)
        .join(" ")
        .toLowerCase()

      return haystack.includes(query)
    })
  }, [allocationFilter, holdings, searchTerm])
  const sectorTreemapItems = useMemo(() => {
    const bySector = new Map()

    for (const holding of holdings) {
      const sectorName = holding.sector || "Unclassified"
      const invested = Number(holding.invested_amount || 0)
      const currentValue = Number(holding.current_value || 0)
      const pnl =
        holding.unrealized_pnl !== null && holding.unrealized_pnl !== undefined
          ? Number(holding.unrealized_pnl || 0)
          : currentValue - invested

      const current = bySector.get(sectorName) || {
        name: sectorName,
        invested: 0,
        currentValue: 0,
        pnl: 0
      }

      current.invested += invested
      current.currentValue += currentValue
      current.pnl += pnl
      bySector.set(sectorName, current)
    }

    const totalCurrent = Array.from(bySector.values()).reduce(
      (sum, item) => sum + item.currentValue,
      0
    )

    return Array.from(bySector.values()).map((item) => ({
      ...item,
      value: item.currentValue,
      weightPercent: totalCurrent > 0 ? (item.currentValue / totalCurrent) * 100 : 0
    }))
  }, [holdings])

  const filterLabel = allocationFilter
    ? `${allocationFilter.type === "asset" ? "Asset" : "Sector"}: ${allocationFilter.value}`
    : null
  const marketTickerItems = useMemo(() => {
    return niftyTickerRows.map((row) => ({
      symbol: row.symbol,
      name: row.name || row.symbol,
      change: Number(row.change_percent || 0)
    }))
  }, [niftyTickerRows])

  const topGainers = useMemo(() => {
    return [...marketTickerItems]
      .filter((item) => Number(item.change || 0) > 0)
      .sort((a, b) => Number(b.change || 0) - Number(a.change || 0))
      .slice(0, 3)
  }, [marketTickerItems])

  const topLosers = useMemo(() => {
    return [...marketTickerItems]
      .filter((item) => Number(item.change || 0) < 0)
      .sort((a, b) => Number(a.change || 0) - Number(b.change || 0))
      .slice(0, 3)
  }, [marketTickerItems])

  const upcomingSipRuns = useMemo(() => {
    return [...recurringSips]
      .filter((sip) => sip.active && sip.next_run_date)
      .sort(
        (a, b) =>
          new Date(a.next_run_date).getTime() - new Date(b.next_run_date).getTime()
      )
      .slice(0, 5)
  }, [recurringSips])

  const monthlySipCommitment = useMemo(() => {
    return recurringSips
      .filter((sip) => sip.active)
      .reduce((sum, sip) => sum + Number(sip.amount || 0), 0)
  }, [recurringSips])

  const rebalanceSignals = useMemo(() => {
    const sectors = sectorAllocation.filter(
      (item) => Number(item.weight_percent || 0) > 0
    )

    if (!sectors.length) {
      return {
        targetWeight: null,
        overweight: [],
        underweight: []
      }
    }

    const targetWeight = 100 / sectors.length
    const threshold = 1
    const withGap = sectors.map((item) => {
      const currentWeight = Number(item.weight_percent || 0)
      return {
        ...item,
        currentWeight,
        gap: currentWeight - targetWeight
      }
    })

    const overweight = withGap
      .filter((item) => item.gap > threshold)
      .sort((a, b) => b.gap - a.gap)
      .slice(0, 3)
    const underweight = withGap
      .filter((item) => item.gap < -threshold)
      .sort((a, b) => a.gap - b.gap)
      .slice(0, 3)

    return {
      targetWeight,
      overweight,
      underweight
    }
  }, [sectorAllocation])

  function jumpToHoldings() {
    const holdingsSection = document.getElementById("holdings-section")
    if (holdingsSection) {
      holdingsSection.scrollIntoView({ behavior: "smooth", block: "start" })
    }
  }

  function handlePriceAlertChange(event) {
    const { name, value } = event.target
    setPriceAlertForm((current) => ({
      ...current,
      [name]: value
    }))
  }

  function handleAuthFormChange(event) {
    const { name, value } = event.target
    setAuthForm((current) => ({
      ...current,
      [name]: value
    }))
  }

  function openAuthModal(mode = "login") {
    setAccountMenuOpen(false)
    setAuthMode(mode)
    setAuthModalOpen(true)
    setAuthForm({
      username: "",
      email: "",
      login: "",
      password: "",
      confirmPassword: ""
    })
  }

  function closeAuthModal() {
    setAuthModalOpen(false)
    setSubmittingAuth(false)
  }

  async function handleAuthSubmit(event) {
    event.preventDefault()
    setSubmittingAuth(true)
    setError("")
    setNotice("")

    try {
      if (authMode === "login") {
        const result = await login({
          login: authForm.login,
          password: authForm.password
        })
        setCurrentUser(result.user)
        setCategory("ALL")
        setNotice(`Welcome back, ${result.user.username}.`)
      } else {
        if (authForm.password !== authForm.confirmPassword) {
          throw new Error("Password and confirm password must match")
        }
        await signup({
          username: authForm.username,
          email: authForm.email,
          password: authForm.password
        })
        const result = await login({
          login: authForm.email,
          password: authForm.password
        })
        setCurrentUser(result.user)
        setCategory("ALL")
        setNotice(`Account created. You are logged in as ${result.user.username}.`)
      }
      closeAuthModal()
    } catch (err) {
      setError(err instanceof Error ? err.message : "Authentication failed")
    } finally {
      setSubmittingAuth(false)
    }
  }

  async function handleLogout() {
    setError("")
    setNotice("")
    try {
      await logout()
    } catch {
      clearStoredSession()
    }
    setAccountMenuOpen(false)
    setCategory("ALL")
    setAllocationFilter(null)
    setCurrentUser(null)
    setNotice("Logged out successfully.")
  }

  return (
    <div className={`app-shell ${themeMode === "light" ? "light-theme" : "dark-theme"}`}>
      {currentUser ? <MarketTicker items={marketTickerItems} /> : null}

      <header className="app-header">
        <div className="header-brand">Portfolio Tracker</div>

        <label className="header-search">
          <input
            type="search"
            placeholder="Search stocks, symbols, sectors..."
            value={searchTerm}
            onChange={(event) => setSearchTerm(event.target.value)}
          />
        </label>

        <div className="header-actions">
          {currentUser ? (
            <>
              <button
                type="button"
                className="header-link theme-toggle"
                onClick={() => setThemeMode((current) => (current === "dark" ? "light" : "dark"))}
              >
                {themeMode === "dark" ? "Light" : "Dark"}
              </button>
              <button
                type="button"
                className="header-link"
                onClick={() => setPriceAlertOpen(true)}
              >
                Price Alerts
              </button>
              <button type="button" className="header-link" onClick={jumpToHoldings}>
                Holdings
              </button>
              <button
                type="button"
                className="icon-button"
                aria-label="Refresh market data"
                onClick={handleRefresh}
                disabled={refreshing || !dashboard}
              >
                <svg viewBox="0 0 24 24" role="presentation" focusable="false">
                  <path d="M17.6 6.3A7 7 0 1 0 19 13h-2a5 5 0 1 1-1-4l-2 2h6V5l-2.4 1.3Z" />
                </svg>
              </button>
            </>
          ) : null}
          <div className="account-wrap" ref={accountMenuRef}>
            <button
              type="button"
              className="header-link"
              onClick={() => setAccountMenuOpen((open) => !open)}
            >
              {currentUser ? currentUser.username : "Login / Signup"}
            </button>
            {accountMenuOpen ? (
              <div className="account-menu">
                {currentUser ? (
                  <button type="button" disabled>
                    Signed in as {currentUser.username}
                  </button>
                ) : (
                  <>
                    <button type="button" onClick={() => openAuthModal("login")}>
                      Login
                    </button>
                    <button type="button" onClick={() => openAuthModal("signup")}>
                      Signup
                    </button>
                  </>
                )}
                {currentUser ? (
                  <button
                    type="button"
                    onClick={() => {
                      uploadInputRef.current?.click()
                      setAccountMenuOpen(false)
                    }}
                  >
                    {uploading ? "Uploading..." : "Upload holdings file"}
                  </button>
                ) : null}
                {currentUser ? (
                  <button
                    type="button"
                    onClick={() => {
                      setAccountMenuOpen(false)
                      handleLogout()
                    }}
                  >
                    Logout
                  </button>
                ) : null}
              </div>
            ) : null}
          </div>
        </div>
      </header>
      <input
        ref={uploadInputRef}
        className="hidden-file-input"
        type="file"
        accept=".xlsx"
        onChange={handleUpload}
        disabled={uploading}
      />

      {error ? <div className="banner error-banner">{error}</div> : null}
      {notice ? <div className="banner success-banner">{notice}</div> : null}

      {!currentUser ? (
        <section className="panel empty-panel">
          <p className="panel-kicker">Authentication required</p>
          <h3>Please login or signup to access your portfolio dashboard.</h3>
          <div className="sip-job-actions">
            <button className="secondary-button" type="button" onClick={() => openAuthModal("login")}>
              Login
            </button>
            <button className="ghost-button" type="button" onClick={() => openAuthModal("signup")}>
              Signup
            </button>
          </div>
        </section>
      ) : loading ? (
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
          <section className="market-overview-grid">
            <div className="market-overview-stack">
              <section className="panel benchmark-panel top-benchmark-panel">
                <div className="panel-heading">
                  <div>
                    <p className="panel-kicker">Benchmarks</p>
                    <h3>Nifty 50 and Sensex today</h3>
                  </div>
                  <span>{overview.as_of ? formatDateTime(overview.as_of) : benchmark?.symbol || "^NSEI"}</span>
                </div>

                <div className="benchmark-top-layout">
                  <div className="benchmark-mini-stack">
                    {benchmarkCharts.map((chart) => (
                      <MiniTrendChart key={chart.symbol} chart={chart} compact />
                    ))}
                  </div>

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
                </div>
              </section>

              <PerformanceChart
                comparison={performanceComparison}
                benchmarkName={benchmark?.name || benchmark?.symbol || "Benchmark"}
                performancePeriod={performancePeriod}
                onPeriodChange={(nextPeriod) => {
                  if (nextPeriod === performancePeriod) {
                    return
                  }
                  setPerformancePeriod(nextPeriod)
                }}
              />
            </div>

            <div className="market-side-panel">
              <MarketSentimentCard benchmark={benchmark} overview={overview} />
              <MarketNewsPanel items={MARKET_NEWS_ITEMS} />
            </div>
          </section>

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
            <article className="summary-card neutral sip-job-card">
              <p>SIP Automation</p>
              <h2>{sipLastRun?.status || "Not run yet"}</h2>
              <span>
                Last run: {sipLastRun?.ended_at ? formatDateTime(sipLastRun.ended_at) : "N/A"} · Total processed:{" "}
                {sipTotals?.processed_sips_total ?? 0}
              </span>
              <span>
                Next schedule: {sipScheduler?.next_run_at ? formatDateTime(sipScheduler.next_run_at) : "Pending"} ·{" "}
                IST {String(sipScheduler?.hour ?? 9).padStart(2, "0")}:{String(sipScheduler?.minute ?? 5).padStart(2, "0")}
              </span>
              <div className="sip-job-actions">
                <button
                  className="secondary-button"
                  type="button"
                  onClick={() => handleRunSipJob(false)}
                  disabled={runningSipJob}
                >
                  {runningSipJob ? "Running..." : "Run now"}
                </button>
                <button
                  className="ghost-button"
                  type="button"
                  onClick={() => handleRunSipJob(true)}
                  disabled={runningSipJob}
                >
                  Force rerun
                </button>
              </div>
            </article>
            <article className="summary-card neutral movers-card">
              <p>Nifty 50 Top Movers</p>
              <h2>Top 3</h2>
              <div className="sip-movers-grid">
                <div>
                  <strong className="sip-movers-title positive">Gainers</strong>
                  {topGainers.length ? (
                    <ul className="sip-movers-list">
                      {topGainers.map((item) => (
                        <li key={`g-${item.symbol}`}>
                          <span>{item.symbol}</span>
                          <b className="mover-positive">{formatPercent(item.change)}</b>
                        </li>
                      ))}
                    </ul>
                  ) : (
                    <p className="empty-state">N/A</p>
                  )}
                </div>
                <div>
                  <strong className="sip-movers-title negative">Losers</strong>
                  {topLosers.length ? (
                    <ul className="sip-movers-list">
                      {topLosers.map((item) => (
                        <li key={`l-${item.symbol}`}>
                          <span>{item.symbol}</span>
                          <b className="mover-negative">{formatPercent(item.change)}</b>
                        </li>
                      ))}
                    </ul>
                  ) : (
                    <p className="empty-state">N/A</p>
                  )}
                </div>
              </div>
            </article>
            <article className="summary-card neutral calendar-card">
              <p>Upcoming SIP Calendar</p>
              <h2>{upcomingSipRuns.length ? `${upcomingSipRuns.length} Upcoming` : "N/A"}</h2>
              <span>Monthly commitment: {formatCurrency(monthlySipCommitment)}</span>
              {upcomingSipRuns.length ? (
                <ul className="insight-list">
                  {upcomingSipRuns.map((sip) => (
                    <li key={`sip-${sip.id}`}>
                      <div>
                        <strong>{sip.symbol}</strong>
                        <small>{formatDate(sip.next_run_date)}</small>
                      </div>
                      <b>{formatCurrency(sip.amount)}</b>
                    </li>
                  ))}
                </ul>
              ) : (
                <p className="empty-state">N/A</p>
              )}
            </article>
            <article className="summary-card neutral rebalance-card">
              <p>Rebalance Signals</p>
              <h2>
                {rebalanceSignals.targetWeight !== null
                  ? `Target ${formatNumber(rebalanceSignals.targetWeight, 2)}%`
                  : "N/A"}
              </h2>
              <span>Equal-weight baseline by sector</span>
              <div className="rebalance-grid">
                <div>
                  <strong className="sip-movers-title negative">Overweight</strong>
                  {rebalanceSignals.overweight.length ? (
                    <ul className="insight-list">
                      {rebalanceSignals.overweight.map((item) => (
                        <li key={`over-${item.name}`}>
                          <div>
                            <strong>{item.name}</strong>
                            <small>Current {formatNumber(item.currentWeight, 2)}%</small>
                          </div>
                          <b className="mover-negative">Trim {formatNumber(item.gap, 2)}%</b>
                        </li>
                      ))}
                    </ul>
                  ) : (
                    <p className="empty-state">N/A</p>
                  )}
                </div>
                <div>
                  <strong className="sip-movers-title positive">Underweight</strong>
                  {rebalanceSignals.underweight.length ? (
                    <ul className="insight-list">
                      {rebalanceSignals.underweight.map((item) => (
                        <li key={`under-${item.name}`}>
                          <div>
                            <strong>{item.name}</strong>
                            <small>Current {formatNumber(item.currentWeight, 2)}%</small>
                          </div>
                          <b className="mover-positive">Add {formatNumber(Math.abs(item.gap), 2)}%</b>
                        </li>
                      ))}
                    </ul>
                  ) : (
                    <p className="empty-state">N/A</p>
                  )}
                </div>
              </div>
            </article>
          </section>

          <section className="panel-grid">
            <AllocationPie
              title="Asset weightage"
              subtitle="Allocation"
              items={assetAllocation}
              filterType="asset"
              activeFilter={allocationFilter}
              onSliceSelect={setAllocationFilter}
            />
            <SectorTreemap
              title="Sector allocation"
              subtitle="Diversification"
              items={sectorTreemapItems}
              activeFilter={allocationFilter}
              onSectorSelect={setAllocationFilter}
            />
          </section>

          <section className="panel" id="holdings-section">
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
      <PriceAlertModal
        open={priceAlertOpen}
        onClose={() => setPriceAlertOpen(false)}
        form={priceAlertForm}
        onChange={handlePriceAlertChange}
      />
      <AuthModal
        open={authModalOpen}
        mode={authMode}
        form={authForm}
        onChange={handleAuthFormChange}
        onClose={closeAuthModal}
        onSwitchMode={() => setAuthMode((current) => (current === "login" ? "signup" : "login"))}
        onSubmit={handleAuthSubmit}
        submitting={submittingAuth}
      />
    </div>
  )
}

export default App
