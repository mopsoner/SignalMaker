import { useState } from 'react'
import { Navigate, NavLink, Route, Routes } from 'react-router-dom'
import DashboardPage from './pages/DashboardPage'
import CandidatesPage from './pages/CandidatesPage'
import PositionsPage from './pages/PositionsPage'
import MomentumExecutorPage from './pages/MomentumExecutorPage'
import OpsPage from './pages/OpsPage'
import LogsPage from './pages/LogsPage'
import AdminSettingsPage from './pages/AdminSettingsPage'
import AssetDetailPage from './pages/AssetDetailPage'

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
          <div className="brand">SignalMaker Executor</div>
          <button className="menu-toggle" type="button" onClick={() => setMenuOpen((v) => !v)}>
            {menuOpen ? 'Close' : 'Menu'}
          </button>
        </div>
        <nav className={`nav-links ${menuOpen ? 'open' : ''}`}>
          <div style={groupTitleStyle}>Momentum</div>
          <NavLink to="/momentum-executor" onClick={closeMenu}>Executor</NavLink>
          <NavLink to="/positions" onClick={closeMenu}>Positions</NavLink>
          <NavLink to="/settings" onClick={closeMenu}>Momentum Settings</NavLink>
          <div style={groupTitleStyle}>Wyckoff / SMC</div>
          <NavLink to="/dashboard" onClick={closeMenu}>Dashboard</NavLink>
          <NavLink to="/candidates" onClick={closeMenu}>Trade Candidates</NavLink>
          <div style={groupTitleStyle}>Admin</div>
          <NavLink to="/ops" onClick={closeMenu}>Ops</NavLink>
          <NavLink to="/logs" onClick={closeMenu}>Logs</NavLink>
        </nav>
      </aside>
      <main className="content" onClick={menuOpen ? closeMenu : undefined}>
        <Routes>
          <Route path="/" element={<Navigate to="/momentum-executor" replace />} />
          <Route path="/dashboard" element={<DashboardPage />} />
          <Route path="/assets/:symbol" element={<AssetDetailPage />} />
          <Route path="/candidates" element={<CandidatesPage />} />
          <Route path="/positions" element={<PositionsPage />} />
          <Route path="/momentum-executor" element={<MomentumExecutorPage />} />
          <Route path="/momentum/admin" element={<MomentumExecutorPage />} />
          <Route path="/ops" element={<OpsPage />} />
          <Route path="/logs" element={<LogsPage />} />
          <Route path="/settings" element={<AdminSettingsPage />} />
        </Routes>
      </main>
    </div>
  )
}
