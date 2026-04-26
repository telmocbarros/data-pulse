ALTER TABLE datasets ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP NULL;

CREATE INDEX IF NOT EXISTS idx_datasets_deleted_at ON datasets(deleted_at) WHERE deleted_at IS NULL;
