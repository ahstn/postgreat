CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_user') THEN
        CREATE ROLE app_user LOGIN PASSWORD 'app_password';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'reader_user') THEN
        CREATE ROLE reader_user LOGIN PASSWORD 'reader_password';
    END IF;
END $$;

GRANT USAGE ON SCHEMA public TO app_user, reader_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO app_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO app_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO reader_user;
