from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Depends, File, Header, HTTPException, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.core.config import settings
from app.models.ticket_sender import TicketFile, TicketSendLog
from app.services.ticket_sender_service import (
    TICKET_STATUSES,
    create_log,
    ensure_safe_ticket_path,
    list_tickets,
    preview_email,
    send_ticket_email,
    serialize_log,
    serialize_ticket,
    upload_pdfs,
)

router = APIRouter()


def require_admin(x_operator_key: str = Header(default="")) -> None:
    if settings.admin_token and x_operator_key != settings.admin_token:
        raise HTTPException(status_code=401, detail="Authentification admin requise")



class TicketUpdatePayload(BaseModel):
    ticketNumber: str | None = None
    eventId: str | None = None
    eventTitle: str | None = None
    eventDescription: str | None = None
    packageId: str | None = None
    packageName: str | None = None
    packageDescription: str | None = None
    orderId: str | None = None
    customerName: str | None = None
    customerEmail: str | None = None
    customerPhone: str | None = None
    status: str | None = None


class EmailPreviewPayload(BaseModel):
    ticketId: int
    body: str | None = None


class SendPayload(BaseModel):
    body: str | None = None


def _get_ticket(db: Session, ticket_id: int) -> TicketFile:
    ticket = db.get(TicketFile, ticket_id)
    if ticket is None:
        raise HTTPException(status_code=404, detail="Ticket introuvable")
    return ticket


@router.post('/admin/tickets/upload')
async def upload_tickets(files: list[UploadFile] = File(...), db: Session = Depends(get_db), _admin: None = Depends(require_admin)) -> dict:
    try:
        payload = []
        for file in files:
            if not file.filename.lower().endswith('.pdf'):
                raise HTTPException(status_code=400, detail="Seuls les fichiers PDF sont acceptés")
            payload.append((file.filename, await file.read()))
        return upload_pdfs(db, payload)
    except HTTPException:
        raise
    except Exception as exc:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(exc))


@router.get('/admin/tickets')
def get_tickets(status: str | None = None, db: Session = Depends(get_db), _admin: None = Depends(require_admin)) -> dict:
    return {"tickets": list_tickets(db, status)}


@router.get('/admin/tickets/{ticket_id}')
def get_ticket(ticket_id: int, db: Session = Depends(get_db), _admin: None = Depends(require_admin)) -> dict:
    return {"ticket": serialize_ticket(_get_ticket(db, ticket_id))}


@router.patch('/admin/tickets/{ticket_id}')
def update_ticket(ticket_id: int, payload: TicketUpdatePayload, db: Session = Depends(get_db), _admin: None = Depends(require_admin)) -> dict:
    ticket = _get_ticket(db, ticket_id)
    data = payload.model_dump(exclude_unset=True)
    if "status" in data and data["status"] not in TICKET_STATUSES:
        raise HTTPException(status_code=400, detail="Statut invalide")
    mapping = {
        "ticketNumber": "ticket_number",
        "eventId": "event_id",
        "eventTitle": "event_title",
        "eventDescription": "event_description",
        "packageId": "package_id",
        "packageName": "package_name",
        "packageDescription": "package_description",
        "orderId": "order_id",
        "customerName": "customer_name",
        "customerEmail": "customer_email",
        "customerPhone": "customer_phone",
        "status": "status",
    }
    for key, value in data.items():
        setattr(ticket, mapping[key], value or "")
    if ticket.status in {"Importé", "À vérifier"} and ticket.customer_email and (ticket.event_title or ticket.event_id) and (ticket.package_name or ticket.package_id):
        ticket.status = "Prêt à envoyer"
    db.commit()
    db.refresh(ticket)
    return {"ticket": serialize_ticket(ticket)}


@router.post('/admin/tickets/preview-email')
def preview_ticket_email(payload: EmailPreviewPayload, db: Session = Depends(get_db), _admin: None = Depends(require_admin)) -> dict:
    return preview_email(_get_ticket(db, payload.ticketId), payload.body)


@router.post('/admin/tickets/{ticket_id}/send')
def send_ticket(ticket_id: int, payload: SendPayload | None = None, db: Session = Depends(get_db), _admin: None = Depends(require_admin)) -> dict:
    try:
        return send_ticket_email(db, _get_ticket(db, ticket_id), payload.body if payload else None, resend=False)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.post('/admin/tickets/{ticket_id}/resend')
def resend_ticket(ticket_id: int, payload: SendPayload | None = None, db: Session = Depends(get_db), _admin: None = Depends(require_admin)) -> dict:
    try:
        return send_ticket_email(db, _get_ticket(db, ticket_id), payload.body if payload else None, resend=True)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.get('/admin/tickets/{ticket_id}/download')
def download_ticket(ticket_id: int, db: Session = Depends(get_db), _admin: None = Depends(require_admin)) -> FileResponse:
    ticket = _get_ticket(db, ticket_id)
    try:
        path = ensure_safe_ticket_path(ticket.stored_file_path)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    if not path.is_file():
        raise HTTPException(status_code=404, detail="PDF du ticket introuvable")
    create_log(db, ticket, "download", "success")
    db.commit()
    filename = f"ticket-{ticket.ticket_number or ticket.id}.pdf"
    return FileResponse(path, media_type="application/pdf", filename=filename)


@router.get('/admin/tickets/{ticket_id}/logs')
def get_ticket_logs(ticket_id: int, db: Session = Depends(get_db), _admin: None = Depends(require_admin)) -> dict:
    _get_ticket(db, ticket_id)
    logs = db.scalars(select(TicketSendLog).where(TicketSendLog.ticket_file_id == ticket_id).order_by(TicketSendLog.created_at.desc())).all()
    return {"logs": [serialize_log(log) for log in logs]}


@router.post('/admin/tickets/{ticket_id}/whatsapp-log')
def whatsapp_log(ticket_id: int, db: Session = Depends(get_db), _admin: None = Depends(require_admin)) -> dict:
    ticket = _get_ticket(db, ticket_id)
    create_log(db, ticket, "whatsapp", "opened")
    db.commit()
    return {"ok": True}
