-- =============================================================
-- Container initialisation script
-- Executed ONCE when the PostgreSQL container is first created.
-- =============================================================

-- Create application schemas
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Grant all privileges on schemas to the application user.
-- POSTGRES_USER is set via the environment and resolves to olist_user.
GRANT ALL PRIVILEGES ON SCHEMA staging   TO olist_user;
GRANT ALL PRIVILEGES ON SCHEMA analytics TO olist_user;

-- Ensure the user can create tables inside the schemas (required for
-- pandas to_sql with if_exists="replace").
ALTER DEFAULT PRIVILEGES IN SCHEMA staging
    GRANT ALL ON TABLES TO olist_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA analytics
    GRANT ALL ON TABLES TO olist_user;
