CREATE TABLE IF NOT EXISTS numeric_profiles (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_id UUID REFERENCES datasets(id) ON DELETE CASCADE,
    column_name VARCHAR(50) NOT NULL,
    min DOUBLE PRECISION,
    max DOUBLE PRECISION,
    sum DOUBLE PRECISION,
    count BIGINT,
    mean DOUBLE PRECISION,
    median DOUBLE PRECISION,
    stdv DOUBLE PRECISION,
    p25 DOUBLE PRECISION,
    p50 DOUBLE PRECISION,
    p75 DOUBLE PRECISION,
    null_count BIGINT,
    null_percent DOUBLE PRECISION,
    UNIQUE (dataset_id, column_name)
);

CREATE TABLE IF NOT EXISTS numeric_profile_histograms (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    numeric_profile_id UUID REFERENCES numeric_profiles(id) ON DELETE CASCADE,
    bucket_min DOUBLE PRECISION,
    bucket_max DOUBLE PRECISION,
    count BIGINT
);

CREATE TABLE IF NOT EXISTS numeric_profile_type_distributions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    numeric_profile_id UUID REFERENCES numeric_profiles(id) ON DELETE CASCADE,
    type_name VARCHAR(50) NOT NULL,
    count DOUBLE PRECISION,
    UNIQUE (numeric_profile_id, type_name)
);

CREATE TABLE IF NOT EXISTS category_profiles (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_id UUID REFERENCES datasets(id) ON DELETE CASCADE,
    column_name VARCHAR(50) NOT NULL,
    cardinality BIGINT,
    uniqueness_ratio DOUBLE PRECISION,
    uniqueness_unique_values BIGINT,
    uniqueness_total_rows BIGINT,
    null_count BIGINT,
    null_percent DOUBLE PRECISION,
    UNIQUE (dataset_id, column_name)
);

CREATE TABLE IF NOT EXISTS category_profile_frequent_values (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    category_profile_id UUID REFERENCES category_profiles(id) ON DELETE CASCADE,
    value TEXT NOT NULL,
    count BIGINT,
    UNIQUE (category_profile_id, value)
);

CREATE TABLE IF NOT EXISTS category_profile_type_distributions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    category_profile_id UUID REFERENCES category_profiles(id) ON DELETE CASCADE,
    type_name VARCHAR(50) NOT NULL,
    count DOUBLE PRECISION,
    UNIQUE (category_profile_id, type_name)
);

CREATE INDEX IF NOT EXISTS idx_numeric_profiles_dataset ON numeric_profiles(dataset_id);
CREATE INDEX IF NOT EXISTS idx_numeric_profile_histograms_profile ON numeric_profile_histograms(numeric_profile_id);
CREATE INDEX IF NOT EXISTS idx_numeric_profile_type_dist_profile ON numeric_profile_type_distributions(numeric_profile_id);
CREATE INDEX IF NOT EXISTS idx_category_profiles_dataset ON category_profiles(dataset_id);
CREATE INDEX IF NOT EXISTS idx_category_profile_freq_values_profile ON category_profile_frequent_values(category_profile_id);
CREATE INDEX IF NOT EXISTS idx_category_profile_type_dist_profile ON category_profile_type_distributions(category_profile_id);
