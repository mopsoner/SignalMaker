import { useState } from 'react'
import { NavLink, Route, Routes } from 'react-router-dom'
import DashboardPage from './pages/DashboardPage'
import CandidatesPage from './pages/CandidatesPage'
import PositionsPage from './pages/PositionsPage'
import MomentumPage from './pages/MomentumPage'
import MomentumCandidatesPage from './pages/MomentumCandidatesPage'
import MomentumBacktestV2Page from './pages/MomentumBacktestV2Page'
import OpsPage from './pages/OpsPage'
import LogsPage from './pages/LogsPage'
import AdminSettingsPage from './pages/AdminSettingsPage'
import AssetDetailPage from './pages/AssetDetailPage'
import ETFStocksPage from './pages/ETFStocksPage'

const groupTitleStyle = {
  margin: '18px 0 8px',
  padding: '0 12px',
  fontSize: 11,
  fontWeight: 800,
  letterSpacing: '0.08em',
  textTransform: 'uppercase',
  color: 'var(--muted)',
}

export default function App() {
  const [menuOpen, setMenuOpen] = useState(false)

  function closeMenu() {
    setMenuOpen(false)
  }

  return (
    <div className="app-shell">
      <aside className={`sidebar ${menuOpen ? 'open' : ''}`}>
        <div className="sidebar-top">
          <div className="brand">SignalMaker</div>
          <button className="menu-toggle" type="button" onClick={() => setMenuOpen((v) => !v)}>
            {menuOpen ? 'Close' : 'Menu'}
          </button>
        </div>
        <nav className={`nav-links ${menuOpen ? 'open' : ''}`}>
          <div style={groupTitleStyle}>Wyckoff / SMC</div>
          <NavLink to="/" end onClick={closeMenu}>Dashboard</NavLink>
          <NavLink to="/positions" onClick={closeMenu}>Positions</NavLink>
          <NavLink to="/candidates" onClick={closeMenu}>Trade Candidates</NavLink>

          <div style={groupTitleStyle}>ETF & Stocks</div>
          <NavLink to="/etf-stocks" onClick={closeMenu}>IBKR Dashboard</NavLink>

          <div style={groupTitleStyle}>Momentum</div>
          <NavLink to="/momentum" onClick={closeMenu}>Dashboard</NavLink>
          <NavLink to="/momentum-candidates" onClick={closeMenu}>Trade Candidates</NavLink>
          <NavLink to="/momentum-backtest" onClick={closeMenu}>Backtest</NavLink>
          <NavLink to="/momentum-etf-stocks" onClick={closeMenu}>ETF & Stocks</NavLink>

          <div style={groupTitleStyle}>Ops / Admin</div>
          <NavLink to="/ops" onClick={closeMenu}>Ops</NavLink>
          <NavLink to="/logs" onClick={closeMenu}>Logs</NavLink>
          <NavLink to="/settings" onClick={closeMenu}>Admin Settings</NavLink>
        </nav>
      </aside>
      <main className="content" onClick={menuOpen ? closeMenu : undefined}>
        <Routes>
          <Route path="/" element={<DashboardPage />} />
          <Route path="/assets/:symbol" element={<AssetDetailPage />} />
          <Route path="/momentum" element={<MomentumPage />} />
          <Route path="/etf-stocks" element={<ETFStocksPage />} />
          <Route path="/momentum-candidates" element={<MomentumCandidatesPage />} />
          <Route path="/momentum-backtest" element={<MomentumBacktestV2Page />} />
          <Route path="/candidates" element={<CandidatesPage />} />
          <Route path="/positions" element={<PositionsPage />} />
          <Route path="/ops" element={<OpsPage />} />
          <Route path="/logs" element={<LogsPage />} />
          <Route path="/settings" element={<AdminSettingsPage />} />
        </Routes>
      </main>
    </div>
  )
}
