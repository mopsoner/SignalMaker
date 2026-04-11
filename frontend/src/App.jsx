import { useState } from 'react'
import { NavLink, Route, Routes } from 'react-router-dom'
import DashboardPage from './pages/DashboardPage'
import CandidatesPage from './pages/CandidatesPage'
import PositionsPage from './pages/PositionsPage'
import OpsPage from './pages/OpsPage'
import LogsPage from './pages/LogsPage'
import AdminSettingsPage from './pages/AdminSettingsPage'

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
          <NavLink to="/" end onClick={closeMenu}>Dashboard</NavLink>
          <NavLink to="/candidates" onClick={closeMenu}>Trade Candidates</NavLink>
          <NavLink to="/positions" onClick={closeMenu}>Positions</NavLink>
          <NavLink to="/ops" onClick={closeMenu}>Ops</NavLink>
          <NavLink to="/logs" onClick={closeMenu}>Logs</NavLink>
          <NavLink to="/settings" onClick={closeMenu}>Admin Settings</NavLink>
        </nav>
      </aside>
      <main className="content" onClick={menuOpen ? closeMenu : undefined}>
        <Routes>
          <Route path="/" element={<DashboardPage />} />
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
