import { useEffect, useState } from "react"
import { getTransactions } from "./services/api"

function App() {
  const [transactions, setTransactions] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState("")

  useEffect(() => {
    let cancelled = false

    async function loadTransactions() {
      setLoading(true)
      setError("")
      try {
        const data = await getTransactions()
        if (!cancelled) {
          setTransactions(Array.isArray(data) ? data : [])
        }
      } catch (err) {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : "Failed to load transactions")
        }
      } finally {
        if (!cancelled) {
          setLoading(false)
        }
      }
    }

    loadTransactions()

    return () => {
      cancelled = true
    }
  }, [])

  return (
    <div style={{ padding: "32px", width: "100%" }}>
      <h1>Portfolio Dashboard</h1>
      <p>Connected to backend API</p>

      {loading && <p>Loading transactions...</p>}
      {error && <p style={{ color: "#c62828" }}>Error: {error}</p>}

      {!loading && !error && (
        <div>
          <h2>Transactions ({transactions.length})</h2>
          {transactions.length === 0 ? (
            <p>No transactions found.</p>
          ) : (
            <table style={{ width: "100%", borderCollapse: "collapse" }}>
              <thead>
                <tr>
                  <th align="left">Date</th>
                  <th align="left">Symbol</th>
                  <th align="right">Type</th>
                  <th align="right">Quantity</th>
                  <th align="right">Price</th>
                </tr>
              </thead>
              <tbody>
                {transactions.map((txn) => (
                  <tr key={txn.id}>
                    <td>{txn.date}</td>
                    <td>{txn.symbol}</td>
                    <td align="right">{txn.type}</td>
                    <td align="right">{txn.quantity}</td>
                    <td align="right">{txn.price}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      )}
    </div>
  )
}

export default App
