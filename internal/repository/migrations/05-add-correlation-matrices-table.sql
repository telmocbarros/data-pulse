CREATE TABLE IF NOT EXISTS correlation_matrices (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_id UUID REFERENCES datasets(id) ON DELETE CASCADE,
    column_a VARCHAR(50) NOT NULL,
    column_b VARCHAR(50) NOT NULL,
    pearson_r DOUBLE PRECISION,
    UNIQUE (dataset_id, column_a, column_b)
);

CREATE INDEX idx_correlation_matrices_dataset ON correlation_matrices(dataset_id);
