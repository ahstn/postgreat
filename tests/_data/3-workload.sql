-- 3-workload.sql
-- Deterministic workload used after pg_stat_statements_reset() in live integration tests.

-- ================================================================
-- 1. Repeated non-indexed filter on rental.return_date
-- ================================================================
DO $$
BEGIN
    FOR i IN 1..20 LOOP
        PERFORM rental_id
        FROM rental
        WHERE return_date > '2005-05-25 00:00:00'::timestamp;
    END LOOP;
END $$;

-- ================================================================
-- 2. Equality filter + ORDER BY to exercise structured evidence
-- ================================================================
DO $$
BEGIN
    FOR i IN 1..12 LOOP
        PERFORM payment_id
        FROM payment
        WHERE customer_id = 42
        ORDER BY payment_date DESC
        LIMIT 25;
    END LOOP;
END $$;

-- ================================================================
-- 3. Temp-heavy join and aggregation workload
-- ================================================================
DO $$
BEGIN
    FOR i IN 1..8 LOOP
        PERFORM *
        FROM (
            SELECT
                c.customer_id,
                SUM(p.amount) AS total_payment,
                COUNT(r.rental_id) AS total_rentals
            FROM customer c
            JOIN payment p ON c.customer_id = p.customer_id
            JOIN rental r ON p.rental_id = r.rental_id
            JOIN inventory i ON r.inventory_id = i.inventory_id
            JOIN film f ON i.film_id = f.film_id
            WHERE f.description LIKE '%Action%'
               OR f.description LIKE '%Drama%'
            GROUP BY c.customer_id
            HAVING SUM(p.amount) > 50
            ORDER BY total_payment DESC
        ) AS temp_heavy_workload;
    END LOOP;
END $$;

-- ================================================================
-- 4. Write-heavy updates to populate WAL metrics
-- ================================================================
DO $$
BEGIN
    FOR i IN 1..12 LOOP
        UPDATE rental
        SET last_update = NOW() + make_interval(secs => i)
        WHERE rental_id BETWEEN 1 AND 100;
    END LOOP;
END $$;
