-- 2-bloat-and-indexes.sql
-- This script generates bloat, creates unused/inefficient indexes, and runs queries to simulate specific access patterns.

-- ==================================================================
-- 1. Generate Bloat in 'rental' table
-- ==================================================================
-- The goal is to create dead tuples.
-- We update a subset of rows multiple times to create dead versions.

DO $$
DECLARE
    i INT;
BEGIN
    FOR i IN 1..50 LOOP
        -- Update approx 10% of the table each time
        UPDATE rental
        SET last_update = NOW()
        WHERE rental_id % 10 = 0;

        -- Delete some rows to create dead tuples that won't be re-used immediately
        IF i % 10 = 0 THEN
             DELETE FROM rental WHERE rental_id % 100 = 1;
        END IF;
    END LOOP;
END $$;

-- ==================================================================
-- 2. Create Unused Index
-- ==================================================================
-- This index is created but we will intentionally NOT query the 'description' column
-- in a way that uses this index (or at all).
CREATE INDEX IF NOT EXISTS idx_film_description_bloat_test ON film(description);

-- ==================================================================
-- 3. Create Inefficient Index (Low Selectivity)
-- ==================================================================
-- 'rating' in film table has few distinct values (G, PG, PG-13, R, NC-17).
-- An index on this is often skipped by the planner in favor of Seq Scan if filtering by a common rating.
CREATE INDEX IF NOT EXISTS idx_film_rating_inefficient ON film(rating);

-- Force some usage of this index if possible, or just let it sit as a potential low-selectivity candidate.
-- To force usage we might need to disable seq scan temporarily, but for the "Analysis" tool
-- we mainly want to see if it flags it as low selectivity if used, or just unused.
-- Let's try to use it a bit.
DO $$
BEGIN
    -- Run a query that uses the index but returns many rows (high idx_tup_read / idx_scan)
    -- modifying enable_seqscan is session local.
    SET enable_seqscan = OFF;
    PERFORM title FROM film WHERE rating = 'PG-13';
    PERFORM title FROM film WHERE rating = 'NC-17';
    SET enable_seqscan = ON;
END $$;

-- ==================================================================
-- 4. Generate Sequential Scans (Missing Index)
-- ==================================================================
-- The 'rental' table has no index on 'return_date'.
-- Frequent queries on this column should trigger "Missing Index" detection.

DO $$
BEGIN
    FOR i IN 1..50 LOOP
        -- This query forces a sequential scan on 'rental'
        PERFORM rental_id
        FROM rental
        WHERE return_date > '2005-05-25 00:00:00'::timestamp;
    END LOOP;
END $$;

-- ==================================================================
-- 5. High Execution Time / Complex Query
-- ==================================================================
-- A complex join with aggregation to consume CPU and time.

DO $$
BEGIN
    FOR i IN 1..5 LOOP
        PERFORM
            c.first_name,
            c.last_name,
            SUM(p.amount) as total_payment,
            COUNT(r.rental_id) as total_rentals
        FROM customer c
        JOIN payment p ON c.customer_id = p.customer_id
        JOIN rental r ON p.rental_id = r.rental_id
        JOIN inventory i ON r.inventory_id = i.inventory_id
        JOIN film f ON i.film_id = f.film_id
        JOIN film_category fc ON f.film_id = fc.film_id
        JOIN category cat ON fc.category_id = cat.category_id
        WHERE f.description LIKE '%Drama%'
           OR f.description LIKE '%Action%'
        GROUP BY c.customer_id, c.first_name, c.last_name
        HAVING SUM(p.amount) > 100
        ORDER BY total_payment DESC;
    END LOOP;
END $$;
