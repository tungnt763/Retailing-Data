CREATE TABLE IF NOT EXISTS audit_load_history (
    id SERIAL PRIMARY KEY,
    run_id TEXT,
    source_system TEXT NOT NULL,
    dest_system TEXT NOT NULL,
    source_name TEXT NOT NULL,
    dest_name TEXT NOT NULL,
    source_created_at TIMESTAMP,
    dest_created_at TIMESTAMP,
    duration FLOAT, -- đơn vị: giây
    record_count INT,
    status TEXT,     -- success | fail | running
    message TEXT,
    load_type TEXT,  -- full/incremental/append
    created_at TIMESTAMP DEFAULT NOW()
);
