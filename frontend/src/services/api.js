const API_BASE = import.meta.env.VITE_API_BASE_URL || "http://127.0.0.1:8000"
const AUTH_TOKEN_KEY = "portfolio_tracker_auth_token"

function getStoredToken() {
  return localStorage.getItem(AUTH_TOKEN_KEY) || ""
}

function buildAuthHeaders(headers = {}) {
  const token = getStoredToken()
  if (!token) {
    return headers
  }
  return {
    ...headers,
    Authorization: `Bearer ${token}`
  }
}

async function request(path, options = {}) {
  const response = await fetch(`${API_BASE}${path}`, {
    ...options,
    headers: buildAuthHeaders(options.headers || {})
  })
  const contentType = response.headers.get("content-type") || ""
  const payload = contentType.includes("application/json")
    ? await response.json()
    : await response.text()

  if (!response.ok) {
    const message =
      typeof payload === "object" && payload !== null
        ? payload.detail || payload.message || JSON.stringify(payload)
        : String(payload)

    throw new Error(message || `Request failed with status ${response.status}`)
  }

  return payload
}

export async function getTransactions() {
  return request("/transactions")
}

export async function createTransaction(data) {
  return request("/transactions", {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify(data)
  })
}

export async function getHoldings() {
  return request("/holdings")
}

export async function getPortfolioValue() {
  return request("/portfolio/value")
}

export async function getPortfolioHistory() {
  return request("/portfolio/history")
}

export async function getPortfolioPerformance() {
  return request("/portfolio/performance")
}

export async function getDailyReturns(limit = 30) {
  return request(`/portfolio/daily-returns?limit=${limit}`)
}

export async function getMaxDrawdown() {
  return request("/portfolio/drawdown")
}

export async function getVolatility() {
  return request("/portfolio/volatility")
}

export async function getSharpeRatio() {
  return request("/portfolio/sharpe")
}

export async function getRollingVolatility(window = 5) {
  return request(`/portfolio/rolling-volatility?window=${window}`)
}

export async function getBeta(benchmark = "^NSEI") {
  return request(`/portfolio/beta?benchmark=${encodeURIComponent(benchmark)}`)
}

export async function getAlpha(benchmark = "^NSEI") {
  return request(`/portfolio/alpha?benchmark=${encodeURIComponent(benchmark)}`)
}

export async function getInformationRatio(benchmark = "^NSEI") {
  return request(
    `/portfolio/information-ratio?benchmark=${encodeURIComponent(benchmark)}`
  )
}

export async function getTrackingError(benchmark = "^NSEI") {
  return request(
    `/portfolio/tracking-error?benchmark=${encodeURIComponent(benchmark)}`
  )
}

export async function refreshPrices() {
  return request("/prices/update", {
    method: "POST"
  })
}

export async function importHoldingsWorkbook(payload) {
  return request("/imports/holdings", {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify(payload)
  })
}

export async function refreshImportedHoldings() {
  return request("/imports/holdings/refresh", {
    method: "POST"
  })
}

export async function applyImportedHoldingTransaction(payload) {
  return request("/imports/holdings/transactions", {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify(payload)
  })
}

export async function addRecurringSip(payload) {
  return request("/imports/holdings/sips", {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify(payload)
  })
}

export async function getImportedDashboard(category = "ALL", performancePeriod = "1Y") {
  return request(
    `/portfolio/imported-dashboard?category=${encodeURIComponent(category)}&performance_period=${encodeURIComponent(performancePeriod)}`
  )
}

export async function getSipJobStatus() {
  return request("/admin/sips/status")
}

export async function runSipJob(force = false) {
  return request(`/admin/sips/run?force=${force ? "true" : "false"}`, {
    method: "POST"
  })
}

export async function getNifty50Snapshot() {
  return request("/market/nifty50")
}

export async function signup(payload) {
  return request("/auth/signup", {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify(payload)
  })
}

export async function login(payload) {
  const response = await request("/auth/login", {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify(payload)
  })
  if (response?.token) {
    localStorage.setItem(AUTH_TOKEN_KEY, response.token)
  }
  return response
}

export async function getCurrentUser() {
  return request("/auth/me")
}

export async function logout() {
  const response = await request("/auth/logout", {
    method: "POST"
  })
  localStorage.removeItem(AUTH_TOKEN_KEY)
  return response
}

export function clearStoredSession() {
  localStorage.removeItem(AUTH_TOKEN_KEY)
}
