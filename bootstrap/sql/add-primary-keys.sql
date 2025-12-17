-- Add primary keys to all T_ tables for CDC upsert mode
-- Run this after pgloader completes

DO $$
DECLARE
    r RECORD;
    table_pk TEXT;
BEGIN
    -- Loop through all T_ tables in xchangelive, xchange_trading, xchange_finance schemas
    FOR r IN
        SELECT schemaname, tablename
        FROM pg_tables
        WHERE schemaname IN ('xchangelive', 'xchange_trading', 'xchange_finance')
          AND tablename LIKE 't_%'
    LOOP
        -- Check if primary key already exists
        SELECT constraint_name INTO table_pk
        FROM information_schema.table_constraints
        WHERE table_schema = r.schemaname
          AND table_name = r.tablename
          AND constraint_type = 'PRIMARY KEY';

        IF table_pk IS NULL THEN
            -- Add primary key on 'id' column
            EXECUTE format('ALTER TABLE %I.%I ADD PRIMARY KEY (id)', r.schemaname, r.tablename);
            RAISE NOTICE 'Added PRIMARY KEY to %.%', r.schemaname, r.tablename;
        ELSE
            RAISE NOTICE 'PRIMARY KEY already exists on %.%', r.schemaname, r.tablename;
        END IF;
    END LOOP;
END $$;
