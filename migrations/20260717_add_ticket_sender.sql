CREATE TABLE IF NOT EXISTS ticket_batches (
    id SERIAL PRIMARY KEY,
    file_name VARCHAR(255) NOT NULL,
    uploaded_by VARCHAR(255) NOT NULL DEFAULT 'admin',
    uploaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status VARCHAR(64) NOT NULL DEFAULT 'Importé'
);

CREATE TABLE IF NOT EXISTS ticket_files (
    id SERIAL PRIMARY KEY,
    batch_id INTEGER NOT NULL REFERENCES ticket_batches(id) ON DELETE CASCADE,
    original_file_name VARCHAR(255) NOT NULL,
    stored_file_path VARCHAR(1024) NOT NULL,
    page_number INTEGER NOT NULL DEFAULT 1,
    ticket_number VARCHAR(128),
    extracted_text TEXT NOT NULL DEFAULT '',
    ocr_text TEXT NOT NULL DEFAULT '',
    ocr_used BOOLEAN NOT NULL DEFAULT FALSE,
    confidence VARCHAR(32) NOT NULL DEFAULT 'low',
    event_id VARCHAR(128),
    event_title VARCHAR(255) NOT NULL DEFAULT '',
    event_description TEXT NOT NULL DEFAULT '',
    package_id VARCHAR(128),
    package_name VARCHAR(255) NOT NULL DEFAULT '',
    package_description TEXT NOT NULL DEFAULT '',
    order_id VARCHAR(128),
    customer_name VARCHAR(255) NOT NULL DEFAULT '',
    customer_email VARCHAR(255) NOT NULL DEFAULT '',
    customer_phone VARCHAR(64) NOT NULL DEFAULT '',
    status VARCHAR(64) NOT NULL DEFAULT 'Importé',
    last_sent_at TIMESTAMPTZ,
    last_error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_ticket_files_batch_id ON ticket_files(batch_id);
CREATE INDEX IF NOT EXISTS ix_ticket_files_status ON ticket_files(status);
CREATE INDEX IF NOT EXISTS ix_ticket_files_ticket_number ON ticket_files(ticket_number);

CREATE TABLE IF NOT EXISTS ticket_send_logs (
    id SERIAL PRIMARY KEY,
    ticket_file_id INTEGER NOT NULL REFERENCES ticket_files(id) ON DELETE CASCADE,
    action VARCHAR(64) NOT NULL,
    status VARCHAR(64) NOT NULL,
    email VARCHAR(255) NOT NULL DEFAULT '',
    phone VARCHAR(64) NOT NULL DEFAULT '',
    subject VARCHAR(500) NOT NULL DEFAULT '',
    error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_ticket_send_logs_ticket_file_id ON ticket_send_logs(ticket_file_id);
CREATE INDEX IF NOT EXISTS ix_ticket_send_logs_created_at ON ticket_send_logs(created_at);
