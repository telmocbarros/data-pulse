CREATE TABLE IF NOT EXISTS datasets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    file_name VARCHAR(50),
    table_name VARCHAR(50),
    size INTEGER,
    uploaded_by VARCHAR(50),
    description VARCHAR(250),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS id_datasets ON datasets(id);
CREATE INDEX IF NOT EXISTS file_name_datasets ON datasets(file_name);
