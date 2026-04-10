import { NavLink, Route, Routes } from 'react-router-dom'
import DashboardPage from './pages/DashboardPage'
import CandidatesPage from './pages/CandidatesPage'
import PositionsPage from './pages/PositionsPage'
import OpsPage from './pages/OpsPage'
import AdminSettingsPage from './pages/AdminSettingsPage'

export default function App() {
  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand">SignalMaker</div>
        <nav className="nav-links">
          <NavLink to="/" end>Dashboard</NavLink>
          <NavLink to="/candidates">Trade Candidates</NavLink>
          <NavLink to="/positions">Positions</NavLink>
          <NavLink to="/ops">Ops</NavLink>
          <NavLink to="/settings">Admin Settings</NavLink>
        </nav>
      </aside>
      <main className="content">
        <Routes>
          <Route path="/" element={<DashboardPage />} />
          <Route path="/candidates" element={<CandidatesPage />} />
          <Route path="/positions" element={<PositionsPage />} />
          <Route path="/ops" element={<OpsPage />} />
          <Route path="/settings" element={<AdminSettingsPage />} />
        </Routes>
      </main>
    </div>
  )
}
