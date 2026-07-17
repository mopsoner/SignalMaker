import { useEffect, useMemo, useState } from 'react'
import { api } from '../lib/api'

const statuts = ['', 'Importé', 'À vérifier', 'Prêt à envoyer', 'Envoyé', 'Échec envoi', 'Relancé']

function emptyForm() {
  return {
    ticketNumber: '', eventId: '', eventTitle: '', eventDescription: '', packageId: '', packageName: '', packageDescription: '', orderId: '', customerName: '', customerEmail: '', customerPhone: '', status: 'Importé',
  }
}

function toForm(ticket) {
  return { ...emptyForm(), ...ticket, ticketNumber: ticket?.ticketNumber || '' }
}

export default function TicketsSenderPage() {
  const [tickets, setTickets] = useState([])
  const [status, setStatus] = useState('')
  const [busy, setBusy] = useState(false)
  const [message, setMessage] = useState('')
  const [selected, setSelected] = useState(null)
  const [form, setForm] = useState(emptyForm())
  const [email, setEmail] = useState({ subject: '', body: '' })
  const [logs, setLogs] = useState([])
  const [csvText, setCsvText] = useState('')

  async function load() {
    const data = await api.tickets(status ? `?status=${encodeURIComponent(status)}` : '')
    setTickets(data.tickets || [])
  }

  useEffect(() => { load().catch((err) => setMessage(err.message)) }, [status])

  async function upload(files) {
    if (!files?.length) return
    setBusy(true); setMessage('Import des PDFs en cours...')
    try {
      const data = await api.uploadTickets(files)
      setMessage(`${data.tickets?.length || 0} ticket(s) importé(s).`)
      await load()
    } catch (err) { setMessage(err.message) } finally { setBusy(false) }
  }

  function open(ticket) {
    setSelected(ticket); setForm(toForm(ticket)); setEmail({ subject: '', body: '' }); setLogs([])
  }

  async function save() {
    if (!selected) return
    setBusy(true)
    try {
      const data = await api.updateTicket(selected.id, form)
      setSelected(data.ticket); setForm(toForm(data.ticket)); setMessage('Ticket mis à jour.'); await load()
    } catch (err) { setMessage(err.message) } finally { setBusy(false) }
  }

  async function preview() {
    if (!selected) return
    try { setEmail(await api.previewTicketEmail({ ticketId: selected.id, body: email.body || undefined })) } catch (err) { setMessage(err.message) }
  }

  async function send(kind = 'send') {
    if (!selected) return
    if (!window.confirm(kind === 'resend' ? 'Renvoyer ce ticket par email ?' : 'Envoyer ce ticket par email ?')) return
    setBusy(true)
    try {
      const data = kind === 'resend' ? await api.resendTicket(selected.id, { body: email.body }) : await api.sendTicket(selected.id, { body: email.body })
      setSelected(data.ticket); setForm(toForm(data.ticket)); setMessage(kind === 'resend' ? 'Email relancé.' : 'Email envoyé.'); await load()
    } catch (err) { setMessage(err.message) } finally { setBusy(false) }
  }

  async function showLogs(ticket = selected) {
    if (!ticket) return
    try { const data = await api.ticketLogs(ticket.id); setLogs(data.logs || []) } catch (err) { setMessage(err.message) }
  }

  async function whatsapp(ticket = selected) {
    if (!ticket) return
    await api.logTicketWhatsapp(ticket.id).catch(() => {})
    const text = `Bonjour, votre ticket pour ${ticket.eventTitle || form.eventTitle} — ${ticket.packageName || form.packageName} vous a été envoyé par email à ${ticket.customerEmail || form.customerEmail}. Pensez à vérifier vos spams si vous ne le trouvez pas.`
    window.open(`https://wa.me/${(ticket.customerPhone || form.customerPhone || '').replace(/\D/g, '')}?text=${encodeURIComponent(text)}`, '_blank', 'noopener,noreferrer')
  }

  function applyCsv() {
    const rows = csvText.split(/\r?\n/).map((line) => line.trim()).filter(Boolean)
    const first = rows[0]?.split(/[;,]/).map((x) => x.trim()) || []
    if (first.length >= 3) setForm((f) => ({ ...f, customerName: `${first[0]} ${first[1]}`.trim(), customerEmail: first[2] || '', customerPhone: first[3] || '' }))
  }

  const selectedTicket = useMemo(() => selected || {}, [selected])

  return <div className="page-stack tickets-page">
    <div className="page-header"><div><h1>Tickets Sender</h1><p>Importer, associer et envoyer des tickets PDF privés depuis l'administration.</p></div></div>
    {message && <div className="panel info">{message}</div>}
    <section className="panel upload-zone">
      <h2>Importer des tickets</h2>
      <p>Déposez un PDF multipage ou plusieurs PDFs mono-ticket. Aucun fichier n'est placé dans un dossier public.</p>
      <input type="file" accept="application/pdf" multiple disabled={busy} onChange={(e) => upload(e.target.files)} />
    </section>
    <section className="panel">
      <div className="market-toolbar"><h2>Tickets analysés</h2><div className="filter-chips">{statuts.map((s) => <button key={s || 'all'} className={`filter-chip ${status === s ? 'active' : ''}`} onClick={() => setStatus(s)}>{s || 'Tous'}</button>)}</div></div>
      <div className="table-wrap"><table className="data-table"><thead><tr><th>Ticket #</th><th>Événement</th><th>Forfait</th><th>Client / email</th><th>Téléphone</th><th>Statut</th><th>Dernier envoi</th><th>Dernière erreur</th><th>Actions</th></tr></thead><tbody>{tickets.map((t) => <tr key={t.id}><td>{t.ticketNumber || 'À vérifier'}</td><td>{t.eventTitle || t.eventId || '—'}</td><td>{t.packageName || t.packageId || '—'}</td><td>{t.customerName}<br />{t.customerEmail || '—'}</td><td>{t.customerPhone || '—'}</td><td><span className="badge blue">{t.status}</span></td><td>{t.lastSentAt || '—'}</td><td>{t.lastError || '—'}</td><td className="ticket-actions"><button onClick={() => open(t)}>Voir détails</button><button onClick={() => { open(t); setTimeout(() => send('resend'), 0) }}>Renvoyer</button><button onClick={() => whatsapp(t)}>WhatsApp</button><a href={api.ticketDownloadUrl(t.id)}>Télécharger</a><button onClick={() => { open(t); showLogs(t) }}>Logs</button></td></tr>)}{!tickets.length && <tr><td colSpan="9" className="empty-cell">Aucun ticket.</td></tr>}</tbody></table></div>
    </section>
    {selected && <section className="panel ticket-detail"><h2>Détails du ticket #{selectedTicket.ticketNumber || selectedTicket.id}</h2><div className="ticket-form-grid">{[
      ['ticketNumber', 'N° ticket'], ['eventId', 'ID événement'], ['eventTitle', 'Titre événement'], ['packageId', 'ID package'], ['packageName', 'Nom package'], ['orderId', 'ID commande'], ['customerName', 'Nom client'], ['customerEmail', 'Email client'], ['customerPhone', 'Téléphone client'],
    ].map(([key, label]) => <label key={key}>{label}<input value={form[key] || ''} onChange={(e) => setForm((f) => ({ ...f, [key]: e.target.value }))} /></label>)}<label>Description événement<textarea value={form.eventDescription || ''} onChange={(e) => setForm((f) => ({ ...f, eventDescription: e.target.value }))} /></label><label>Description package<textarea value={form.packageDescription || ''} onChange={(e) => setForm((f) => ({ ...f, packageDescription: e.target.value }))} /></label><label>Statut<select value={form.status} onChange={(e) => setForm((f) => ({ ...f, status: e.target.value }))}>{statuts.filter(Boolean).map((s) => <option key={s}>{s}</option>)}</select></label></div><div className="two-col"><div><h3>Client externe CSV</h3><p>Format : prénom;nom;email;téléphone. Le champ existant customerName reçoit prénom + nom.</p><textarea value={csvText} onChange={(e) => setCsvText(e.target.value)} /><button className="button" onClick={applyCsv}>Appliquer la première ligne</button></div><div><h3>Aperçu email</h3><p><strong>Sujet :</strong> {email.subject || 'Cliquez sur prévisualiser.'}</p><textarea value={email.body} placeholder="Corps de l'email en français" onChange={(e) => setEmail((m) => ({ ...m, body: e.target.value }))} /></div></div><div className="page-actions"><button className="button" disabled={busy} onClick={save}>Enregistrer</button><button className="button" onClick={preview}>Prévisualiser l'email</button><button className="button" disabled={busy} onClick={() => send('send')}>Envoyer</button><button className="button" disabled={busy} onClick={() => send('resend')}>Renvoyer</button><button className="button" onClick={() => whatsapp()}>WhatsApp</button><a className="button" href={api.ticketDownloadUrl(selected.id)}>Télécharger le ticket</a><button className="button" onClick={() => showLogs()}>Voir logs</button></div>{logs.length > 0 && <div className="table-wrap"><table className="data-table"><thead><tr><th>Date</th><th>Action</th><th>Statut</th><th>Email</th><th>Erreur</th></tr></thead><tbody>{logs.map((l) => <tr key={l.id}><td>{l.createdAt}</td><td>{l.action}</td><td>{l.status}</td><td>{l.email}</td><td>{l.error || '—'}</td></tr>)}</tbody></table></div>}</section>}
  </div>
}
