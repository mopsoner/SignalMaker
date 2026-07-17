from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base


class TicketBatch(Base):
    __tablename__ = "ticket_batches"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    file_name: Mapped[str] = mapped_column(String(255), nullable=False)
    uploaded_by: Mapped[str] = mapped_column(String(255), default="admin", nullable=False)
    uploaded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    status: Mapped[str] = mapped_column(String(64), default="Importé", nullable=False)

    tickets: Mapped[list["TicketFile"]] = relationship("TicketFile", back_populates="batch")


class TicketFile(Base):
    __tablename__ = "ticket_files"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    batch_id: Mapped[int] = mapped_column(ForeignKey("ticket_batches.id", ondelete="CASCADE"), index=True, nullable=False)
    original_file_name: Mapped[str] = mapped_column(String(255), nullable=False)
    stored_file_path: Mapped[str] = mapped_column(String(1024), nullable=False)
    page_number: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    ticket_number: Mapped[str | None] = mapped_column(String(128), nullable=True)
    extracted_text: Mapped[str] = mapped_column(Text, default="", nullable=False)
    ocr_text: Mapped[str] = mapped_column(Text, default="", nullable=False)
    ocr_used: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    confidence: Mapped[str] = mapped_column(String(32), default="low", nullable=False)
    event_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    event_title: Mapped[str] = mapped_column(String(255), default="", nullable=False)
    event_description: Mapped[str] = mapped_column(Text, default="", nullable=False)
    package_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    package_name: Mapped[str] = mapped_column(String(255), default="", nullable=False)
    package_description: Mapped[str] = mapped_column(Text, default="", nullable=False)
    order_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    customer_name: Mapped[str] = mapped_column(String(255), default="", nullable=False)
    customer_email: Mapped[str] = mapped_column(String(255), default="", nullable=False)
    customer_phone: Mapped[str] = mapped_column(String(64), default="", nullable=False)
    status: Mapped[str] = mapped_column(String(64), default="Importé", nullable=False)
    last_sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error: Mapped[str] = mapped_column(Text, default="", nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    batch: Mapped[TicketBatch] = relationship("TicketBatch", back_populates="tickets")
    logs: Mapped[list["TicketSendLog"]] = relationship("TicketSendLog", back_populates="ticket")


class TicketSendLog(Base):
    __tablename__ = "ticket_send_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    ticket_file_id: Mapped[int] = mapped_column(ForeignKey("ticket_files.id", ondelete="CASCADE"), index=True, nullable=False)
    action: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(64), nullable=False)
    email: Mapped[str] = mapped_column(String(255), default="", nullable=False)
    phone: Mapped[str] = mapped_column(String(64), default="", nullable=False)
    subject: Mapped[str] = mapped_column(String(500), default="", nullable=False)
    error: Mapped[str] = mapped_column(Text, default="", nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    ticket: Mapped[TicketFile] = relationship("TicketFile", back_populates="logs")
