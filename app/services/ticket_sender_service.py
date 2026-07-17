from __future__ import annotations

import io
import os
import re
import smtplib
from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path
from typing import Iterable

from pypdf import PdfReader, PdfWriter
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import settings
from app.models.ticket_sender import TicketBatch, TicketFile, TicketSendLog

TICKET_STATUSES = {"Importé", "À vérifier", "Prêt à envoyer", "Envoyé", "Échec envoi", "Relancé"}
PRIVATE_ROOT = Path("private/tickets").resolve()


def private_ticket_root() -> Path:
    PRIVATE_ROOT.mkdir(parents=True, exist_ok=True)
    return PRIVATE_ROOT


def ensure_safe_ticket_path(path_value: str) -> Path:
    root = private_ticket_root()
    path = Path(path_value).resolve()
    if root not in path.parents:
        raise ValueError("Chemin de ticket non autorisé")
    return path


def detect_ticket_number(text: str) -> tuple[str | None, str]:
    cleaned = " ".join((text or "").split())
    patterns = [
        (r"(?:Ticket\s*(?:#|No\.?|N[o°º]?|num[eé]ro)\s*[:\-]?)\s*([A-Z0-9][A-Z0-9\-_/]{4,})", "high"),
        (r"(?:N[°º]\s*[:\-]?)\s*([A-Z0-9][A-Z0-9\-_/]{4,})", "high"),
        (r"(?:Order|Commande|R[eé]f[eé]rence)\s*(?:#|No\.?|N[o°º]?|:)?\s*([A-Z0-9][A-Z0-9\-_/]{5,})", "medium"),
        (r"\b([A-Z]{2,}[A-Z0-9]{6,}|[0-9]{8,}|[A-Z0-9]{4,}-[A-Z0-9]{4,}(?:-[A-Z0-9]{3,})?)\b", "low"),
    ]
    for pattern, confidence in patterns:
        match = re.search(pattern, cleaned, flags=re.IGNORECASE)
        if match:
            return match.group(1).strip().upper(), confidence
    return None, "low"


def serialize_ticket(ticket: TicketFile) -> dict:
    return {
        "id": ticket.id,
        "batchId": ticket.batch_id,
        "originalFileName": ticket.original_file_name,
        "pageNumber": ticket.page_number,
        "ticketNumber": ticket.ticket_number,
        "extractedTextPreview": (ticket.extracted_text or ticket.ocr_text or "")[:500],
        "ocrUsed": ticket.ocr_used,
        "confidence": ticket.confidence,
        "eventId": ticket.event_id or "",
        "eventTitle": ticket.event_title,
        "eventDescription": ticket.event_description,
        "packageId": ticket.package_id or "",
        "packageName": ticket.package_name,
        "packageDescription": ticket.package_description,
        "orderId": ticket.order_id or "",
        "customerName": ticket.customer_name,
        "customerEmail": ticket.customer_email,
        "customerPhone": ticket.customer_phone,
        "status": ticket.status,
        "lastSentAt": ticket.last_sent_at.isoformat() if ticket.last_sent_at else None,
        "lastError": ticket.last_error,
        "createdAt": ticket.created_at.isoformat() if ticket.created_at else None,
        "updatedAt": ticket.updated_at.isoformat() if ticket.updated_at else None,
    }


def serialize_log(log: TicketSendLog) -> dict:
    return {
        "id": log.id,
        "ticketFileId": log.ticket_file_id,
        "action": log.action,
        "status": log.status,
        "email": log.email,
        "phone": log.phone,
        "subject": log.subject,
        "error": log.error,
        "createdAt": log.created_at.isoformat() if log.created_at else None,
    }


def create_log(db: Session, ticket: TicketFile, action: str, status: str, subject: str = "", error: str = "") -> TicketSendLog:
    log = TicketSendLog(
        ticket_file_id=ticket.id,
        action=action,
        status=status,
        email=ticket.customer_email or "",
        phone=ticket.customer_phone or "",
        subject=subject or "",
        error=error or "",
    )
    db.add(log)
    return log


def upload_pdfs(db: Session, files: Iterable[tuple[str, bytes]], uploaded_by: str = "admin") -> dict:
    materialized = [(name, data) for name, data in files]
    if not materialized:
        raise ValueError("Aucun fichier PDF fourni")
    batch = TicketBatch(file_name=", ".join(name for name, _ in materialized)[:255], uploaded_by=uploaded_by, status="Importé")
    db.add(batch)
    db.flush()
    batch_dir = private_ticket_root() / str(batch.id)
    batch_dir.mkdir(parents=True, exist_ok=True)
    created: list[TicketFile] = []

    for original_name, content in materialized:
        reader = PdfReader(io.BytesIO(content))
        for index, page in enumerate(reader.pages, start=1):
            writer = PdfWriter()
            writer.add_page(page)
            output = io.BytesIO()
            writer.write(output)
            extracted_text = page.extract_text() or ""
            number, confidence = detect_ticket_number(extracted_text)
            status = "Importé" if confidence in {"high", "medium"} else "À vérifier"
            ticket = TicketFile(
                batch_id=batch.id,
                original_file_name=original_name,
                stored_file_path="",
                page_number=index,
                ticket_number=number,
                extracted_text=extracted_text,
                ocr_text="",
                ocr_used=False,
                confidence=confidence,
                status=status,
            )
            db.add(ticket)
            db.flush()
            ticket_path = batch_dir / f"{ticket.id}.pdf"
            ticket_path.write_bytes(output.getvalue())
            ticket.stored_file_path = str(ticket_path)
            created.append(ticket)
    db.commit()
    return {"batchId": batch.id, "tickets": [serialize_ticket(t) for t in created]}


def preview_email(ticket: TicketFile, body_override: str | None = None) -> dict:
    subject = f"Votre ticket — {ticket.event_title} — {ticket.package_name}"
    if ticket.ticket_number:
        subject += f" — Ticket #{ticket.ticket_number}"
    body = body_override or (
        f"Bonjour,\n\n"
        f"Votre ticket PDF est joint à cet email.\n\n"
        f"Événement : {ticket.event_title}\n"
        f"Package : {ticket.package_name}\n\n"
        f"Merci de présenter le PDF joint à l'entrée et de conserver une copie accessible le jour de l'événement.\n\n"
        f"Pour toute question, contactez notre support en répondant à cet email.\n\n"
        f"Cordialement."
    )
    return {"subject": subject, "body": body}


def validate_ready_to_send(ticket: TicketFile) -> None:
    if not ticket.customer_email:
        raise ValueError("Email client obligatoire")
    if not ticket.event_title and not ticket.event_id:
        raise ValueError("Événement obligatoire")
    if not ticket.package_name and not ticket.package_id:
        raise ValueError("Package obligatoire")
    path = ensure_safe_ticket_path(ticket.stored_file_path)
    if not path.is_file():
        raise ValueError("PDF du ticket introuvable")
    if ticket.status == "À vérifier" or ticket.confidence == "low":
        raise ValueError("Validation manuelle requise pour ce ticket")


def send_ticket_email(db: Session, ticket: TicketFile, body_override: str | None = None, resend: bool = False) -> dict:
    preview = preview_email(ticket, body_override)
    try:
        validate_ready_to_send(ticket)
        path = ensure_safe_ticket_path(ticket.stored_file_path)
        _send_email_with_attachment(ticket.customer_email, preview["subject"], preview["body"], path)
        ticket.last_sent_at = datetime.now(timezone.utc)
        ticket.last_error = ""
        ticket.status = "Relancé" if resend else "Envoyé"
        create_log(db, ticket, "resend" if resend else "send", "success", preview["subject"])
        db.commit()
        return {"ok": True, **preview, "ticket": serialize_ticket(ticket)}
    except Exception as exc:
        ticket.last_error = str(exc)
        ticket.status = "Échec envoi"
        create_log(db, ticket, "resend" if resend else "send", "error", preview["subject"], str(exc))
        db.commit()
        raise


def _send_email_with_attachment(to_email: str, subject: str, body: str, pdf_path: Path) -> None:
    smtp_host = getattr(settings, "smtp_host", "")
    if not smtp_host:
        raise RuntimeError("SMTP_HOST n'est pas configuré")
    msg = EmailMessage()
    msg["From"] = getattr(settings, "smtp_from", "tickets@example.com")
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.set_content(body)
    msg.add_attachment(pdf_path.read_bytes(), maintype="application", subtype="pdf", filename=pdf_path.name)
    port = int(getattr(settings, "smtp_port", 587))
    username = getattr(settings, "smtp_username", "")
    password = getattr(settings, "smtp_password", "")
    with smtplib.SMTP(smtp_host, port, timeout=20) as smtp:
        if getattr(settings, "smtp_use_tls", True):
            smtp.starttls()
        if username:
            smtp.login(username, password)
        smtp.send_message(msg)


def list_tickets(db: Session, status: str | None = None) -> list[dict]:
    stmt = select(TicketFile).order_by(TicketFile.created_at.desc(), TicketFile.id.desc())
    if status:
        stmt = stmt.where(TicketFile.status == status)
    return [serialize_ticket(t) for t in db.scalars(stmt).all()]
