import { useCallback, useEffect, useMemo, useState } from "react"
import {
  getImportedDashboard,
  importHoldingsWorkbook,
  refreshImportedHoldings
} from "./services/api"
import "./App.css"

const PIE_COLORS = [
  "#f7bf58",
  "#68cf9c",
  "#6da7ff",
  "#ff8175",
  "#c89dff",
  "#8bd8ff",
  "#ffd88a",
  "#ffbca7"
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

function describeArc(centerX, centerY, radius, startAngle, endAngle) {
  const start = polarToCartesian(centerX, centerY, radius, endAngle)
  const end = polarToCartesian(centerX, centerY, radius, startAngle)
  const largeArcFlag = endAngle - startAngle <= 180 ? "0" : "1"
  return [
    "M",
    start.x,
    start.y,
    "A",
    radius,
    radius,
    0,
    largeArcFlag,
    0,
    end.x,
    end.y
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
      path: describeArc(120, 120, 74, previousAngle, endAngle),
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
            <circle cx="120" cy="120" r="74" fill="none" stroke="rgba(255,255,255,0.06)" strokeWidth="34" />
            {slices.map((slice) => (
              <path
                key={slice.name}
                d={slice.path}
                fill="none"
                stroke={slice.color}
                strokeWidth={slice.isActive ? 40 : 34}
                strokeLinecap="round"
                className="pie-slice"
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

function App() {
  const [category, setCategory] = useState("ALL")
  const [dashboard, setDashboard] = useState(null)
  const [loading, setLoading] = useState(true)
  const [refreshing, setRefreshing] = useState(false)
  const [uploading, setUploading] = useState(false)
  const [error, setError] = useState("")
  const [notice, setNotice] = useState("")
  const [allocationFilter, setAllocationFilter] = useState(null)

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

  const overview = dashboard?.overview
  const holdings = useMemo(() => dashboard?.holdings ?? [], [dashboard])
  const benchmark = dashboard?.benchmark
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
      ) : !dashboard || !holdings.length ? (
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

            <div className="metric-grid">
              <div className="metric-card">
                <span>Index level</span>
                <strong>{benchmark?.price ? formatNumber(benchmark.price, 2) : "N/A"}</strong>
              </div>
              <div className="metric-card">
                <span>1D move</span>
                <strong className={statusTone(benchmark?.one_day_change_percent ?? 0)}>
                  {benchmark?.one_day_change_percent !== null ? formatPercent(benchmark.one_day_change_percent) : "N/A"}
                </strong>
              </div>
              <div className="metric-card">
                <span>Index P/E</span>
                <strong>{benchmark?.pe_ratio !== null ? formatNumber(benchmark.pe_ratio, 2) : "N/A"}</strong>
              </div>
              <div className="metric-card">
                <span>Portfolio vs index 1D</span>
                <strong className={statusTone((overview.one_day_change_percent ?? 0) - (benchmark?.one_day_change_percent ?? 0))}>
                  {benchmark?.one_day_change_percent !== null
                    ? formatPercent(overview.one_day_change_percent - benchmark.one_day_change_percent)
                    : "N/A"}
                </strong>
              </div>
            </div>
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
                      </tr>
                    ))
                  ) : (
                    <tr>
                      <td colSpan="10" className="empty-state">
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
    </div>
  )
}

export default App
