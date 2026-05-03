CREATE TABLE IF NOT EXISTS dataset_validation_errors (
    id BIGSERIAL PRIMARY KEY,
    dataset_id UUID NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
    row_number INTEGER NOT NULL,
    column_index INTEGER NOT NULL,
    kind VARCHAR(50) NOT NULL,
    expected TEXT,
    received TEXT,
    detail TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dataset_validation_errors_dataset_id
    ON dataset_validation_errors(dataset_id, id);
