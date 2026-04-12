CREATE TABLE IF NOT EXISTS dataset_columns(
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_id UUID REFERENCES datasets(id) ON DELETE CASCADE,
    column_name varchar(50),
    column_type varchar(50),
    UNIQUE (dataset_id, column_name)
);

CREATE INDEX dataset_id_dataset_columns ON dataset_columns(dataset_id);