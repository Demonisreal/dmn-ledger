CREATE TABLE accounts (
    id              bigint      PRIMARY KEY,
    ledger          integer     NOT NULL,
    code            smallint    NOT NULL,
    flags           smallint    NOT NULL DEFAULT 0,
    debits_pending  bigint      NOT NULL DEFAULT 0,
    debits_posted   bigint      NOT NULL DEFAULT 0,
    credits_pending bigint      NOT NULL DEFAULT 0,
    credits_posted  bigint      NOT NULL DEFAULT 0,
    created_at      timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT balances_not_negative CHECK (
        debits_pending >= 0 AND debits_posted >= 0 AND credits_pending >= 0 AND credits_posted >= 0),
    CONSTRAINT debits_within_credits CHECK (
        flags & 1 = 0 OR debits_posted + debits_pending <= credits_posted),
    CONSTRAINT credits_within_debits CHECK (
        flags & 2 = 0 OR credits_posted + credits_pending <= debits_posted)
) WITH (fillfactor = 70);

CREATE TABLE transfers (
    id                uuid        PRIMARY KEY,
    debit_account_id  bigint      NOT NULL REFERENCES accounts (id),
    credit_account_id bigint      NOT NULL REFERENCES accounts (id),
    amount            bigint      NOT NULL CHECK (amount > 0),
    pending_id        uuid        REFERENCES transfers (id),
    ledger            integer     NOT NULL,
    code              smallint    NOT NULL,
    state             smallint    NOT NULL DEFAULT 0,
    created_at        timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (debit_account_id <> credit_account_id)
);

CREATE FUNCTION post_locked(tid uuid, dr bigint, cr bigint, amt bigint, ldg integer, cd smallint)
RETURNS boolean LANGUAGE plpgsql AS $$
BEGIN
    PERFORM 1 FROM accounts WHERE id IN (dr, cr) ORDER BY id FOR NO KEY UPDATE;
    INSERT INTO transfers (id, debit_account_id, credit_account_id, amount, ledger, code)
    VALUES (tid, dr, cr, amt, ldg, cd)
    ON CONFLICT (id) DO NOTHING;
    IF NOT FOUND THEN
        RETURN false;
    END IF;
    UPDATE accounts SET debits_posted = debits_posted + amt WHERE id = dr;
    UPDATE accounts SET credits_posted = credits_posted + amt WHERE id = cr;
    RETURN true;
END $$;

CREATE FUNCTION post_batch_locked(tids uuid[], drs bigint[], crs bigint[], amts bigint[],
                                  ldg integer, cd smallint)
RETURNS SETOF uuid LANGUAGE plpgsql AS $$
BEGIN
    PERFORM 1 FROM accounts WHERE id = ANY (drs || crs) ORDER BY id FOR NO KEY UPDATE;
    RETURN QUERY
    WITH fresh AS (
        INSERT INTO transfers (id, debit_account_id, credit_account_id, amount, ledger, code)
        SELECT t.id, t.dr, t.cr, t.amt, ldg, cd
        FROM unnest(tids, drs, crs, amts) AS t (id, dr, cr, amt)
        ON CONFLICT (id) DO NOTHING
        RETURNING id, debit_account_id, credit_account_id, amount
    ), moved AS (
        SELECT account, sum(debit)::bigint AS debit, sum(credit)::bigint AS credit
        FROM (
            SELECT debit_account_id AS account, amount AS debit, 0::bigint AS credit FROM fresh
            UNION ALL
            SELECT credit_account_id, 0::bigint, amount FROM fresh
        ) AS sides
        GROUP BY account
    ), booked AS (
        UPDATE accounts a
        SET debits_posted = a.debits_posted + m.debit,
            credits_posted = a.credits_posted + m.credit
        FROM moved m
        WHERE a.id = m.account
    )
    SELECT id FROM fresh;
END $$;

CREATE FUNCTION reserve_locked(tid uuid, dr bigint, cr bigint, amt bigint, ldg integer, cd smallint)
RETURNS boolean LANGUAGE plpgsql AS $$
BEGIN
    PERFORM 1 FROM accounts WHERE id IN (dr, cr) ORDER BY id FOR NO KEY UPDATE;
    INSERT INTO transfers (id, debit_account_id, credit_account_id, amount, ledger, code, state)
    VALUES (tid, dr, cr, amt, ldg, cd, 1)
    ON CONFLICT (id) DO NOTHING;
    IF NOT FOUND THEN
        RETURN false;
    END IF;
    UPDATE accounts SET debits_pending = debits_pending + amt WHERE id = dr;
    UPDATE accounts SET credits_pending = credits_pending + amt WHERE id = cr;
    RETURN true;
END $$;

CREATE FUNCTION settle_locked(tid uuid, pid uuid, post boolean)
RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE
    p transfers%ROWTYPE;
BEGIN
    SELECT * INTO p FROM transfers WHERE id = pid FOR NO KEY UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'pending transfer % not found', pid USING ERRCODE = 'no_data_found';
    END IF;
    PERFORM 1 FROM accounts WHERE id IN (p.debit_account_id, p.credit_account_id) ORDER BY id FOR NO KEY UPDATE;
    INSERT INTO transfers (id, debit_account_id, credit_account_id, amount, pending_id, ledger, code)
    VALUES (tid, p.debit_account_id, p.credit_account_id, p.amount, pid, p.ledger, p.code)
    ON CONFLICT (id) DO NOTHING;
    IF NOT FOUND THEN
        RETURN false;
    END IF;
    IF p.state <> 1 THEN
        RAISE EXCEPTION 'transfer % is not open', pid USING ERRCODE = 'raise_exception';
    END IF;
    UPDATE transfers SET state = CASE WHEN post THEN 2 ELSE 3 END WHERE id = pid;
    UPDATE accounts
    SET debits_pending = debits_pending - p.amount,
        debits_posted = debits_posted + CASE WHEN post THEN p.amount ELSE 0 END
    WHERE id = p.debit_account_id;
    UPDATE accounts
    SET credits_pending = credits_pending - p.amount,
        credits_posted = credits_posted + CASE WHEN post THEN p.amount ELSE 0 END
    WHERE id = p.credit_account_id;
    RETURN true;
END $$;

CREATE FUNCTION twophase_batch_locked(tids uuid[], pids uuid[], drs bigint[], crs bigint[], amts bigint[],
                                      posts boolean[], ldg integer, cd smallint)
RETURNS SETOF uuid LANGUAGE plpgsql AS $$
BEGIN
    PERFORM 1 FROM transfers WHERE id = ANY (pids) ORDER BY id FOR NO KEY UPDATE;
    PERFORM 1 FROM accounts WHERE id = ANY (drs || crs) ORDER BY id FOR NO KEY UPDATE;
    IF EXISTS (
        SELECT 1
        FROM unnest(tids, pids, drs, crs) AS b (id, pid, dr, cr)
        LEFT JOIN transfers p ON p.id = b.pid
        WHERE b.pid IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM transfers d WHERE d.id = b.id)
          AND (p.state IS DISTINCT FROM 1 OR p.debit_account_id <> b.dr OR p.credit_account_id <> b.cr)
    ) OR (
        SELECT count(DISTINCT b.pid) < count(DISTINCT b.id)
        FROM unnest(tids, pids) AS b (id, pid)
        WHERE b.pid IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM transfers d WHERE d.id = b.id)
    ) THEN
        RAISE EXCEPTION 'batch names a pending transfer that is not open' USING ERRCODE = 'raise_exception';
    END IF;
    RETURN QUERY
    WITH fresh AS (
        INSERT INTO transfers (id, debit_account_id, credit_account_id, amount, pending_id, ledger, code, state)
        SELECT b.id, b.dr, b.cr, coalesce(p.amount, b.amt), b.pid, ldg, cd, CASE WHEN b.pid IS NULL THEN 1 ELSE 0 END
        FROM unnest(tids, pids, drs, crs, amts) WITH ORDINALITY AS b (id, pid, dr, cr, amt, ord)
        LEFT JOIN transfers p ON p.id = b.pid
        ORDER BY b.ord
        ON CONFLICT (id) DO NOTHING
        RETURNING id, pending_id, debit_account_id, credit_account_id, amount
    ), moves AS (
        SELECT DISTINCT ON (b.id) b.id, b.post
        FROM unnest(tids, posts) WITH ORDINALITY AS b (id, post, ord)
        ORDER BY b.id, b.ord
    ), steps AS (
        SELECT f.id, f.pending_id, f.debit_account_id, f.credit_account_id, f.amount,
               f.pending_id IS NOT NULL AND m.post AS post
        FROM fresh f
        JOIN moves m ON m.id = f.id
    ), closed AS (
        UPDATE transfers t
        SET state = CASE WHEN s.post THEN 2 ELSE 3 END
        FROM steps s
        WHERE t.id = s.pending_id
    ), moved AS (
        SELECT account, sum(dp)::bigint AS dp, sum(dq)::bigint AS dq, sum(cp)::bigint AS cp, sum(cq)::bigint AS cq
        FROM (
            SELECT debit_account_id AS account,
                   CASE WHEN pending_id IS NULL THEN amount ELSE -amount END AS dp,
                   CASE WHEN post THEN amount ELSE 0 END AS dq,
                   0::bigint AS cp,
                   0::bigint AS cq
            FROM steps
            UNION ALL
            SELECT credit_account_id, 0::bigint, 0::bigint,
                   CASE WHEN pending_id IS NULL THEN amount ELSE -amount END,
                   CASE WHEN post THEN amount ELSE 0 END
            FROM steps
        ) AS sides
        GROUP BY account
    ), booked AS (
        UPDATE accounts a
        SET debits_pending = a.debits_pending + m.dp,
            debits_posted = a.debits_posted + m.dq,
            credits_pending = a.credits_pending + m.cp,
            credits_posted = a.credits_posted + m.cq
        FROM moved m
        WHERE a.id = m.account
    )
    SELECT id FROM fresh;
END $$;
