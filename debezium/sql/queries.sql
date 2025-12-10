-- Debezium CDC Demo: Example Queries
-- These queries demonstrate XTDB's capabilities after ingesting CDC events

-- =============================================================================
-- 1. CURRENT STATE QUERIES
-- =============================================================================

-- Current users (notice varying column presence due to schema evolution)
\echo '\n=== Current Users ==='
SELECT _id, email, username, phone_number, verified_at, created_at
FROM users
ORDER BY _id;

-- Current profiles
\echo '\n=== Current Profiles ==='
SELECT _id, user_id, display_name, avatar_url, bio
FROM profiles
ORDER BY _id;

-- Current sessions
\echo '\n=== Current Sessions ==='
SELECT _id, user_id, token, device_type, ip_address, created_at
FROM sessions
ORDER BY _id;

-- =============================================================================
-- 2. SCHEMA EVOLUTION EVIDENCE
-- =============================================================================

-- Show that early records don't have new columns (they're NULL)
-- while later records have them populated
\echo '\n=== Schema Evolution: Users with/without phone_number ==='
SELECT
    _id,
    username,
    phone_number,
    CASE
        WHEN phone_number IS NULL THEN 'Before ALTER TABLE'
        ELSE 'After ALTER TABLE'
    END as schema_version
FROM users
ORDER BY _id;

-- =============================================================================
-- 3. TIME-TRAVEL QUERIES (Bitemporality)
-- =============================================================================

-- See all historical versions of user id=1 (Alice)
\echo '\n=== Historical Versions of Alice (user id=1) ==='
SELECT
    _id,
    username,
    email,
    phone_number,
    verified_at,
    _valid_from,
    _valid_to
FROM users FOR ALL VALID_TIME
WHERE _id = 1
ORDER BY _valid_from;

-- See Bob's email change over time
\echo '\n=== Bob Email Change History (user id=2) ==='
SELECT
    _id,
    username,
    email,
    _valid_from
FROM users FOR ALL VALID_TIME
WHERE _id = 2
ORDER BY _valid_from;

-- See deleted users (Charlie was deactivated)
\echo '\n=== Deleted Users (visible with ALL VALID_TIME) ==='
SELECT
    _id,
    username,
    email,
    _valid_from,
    _valid_to
FROM users FOR ALL VALID_TIME
WHERE _valid_to IS NOT NULL
ORDER BY _id;

-- =============================================================================
-- 4. POINT-IN-TIME QUERIES
-- =============================================================================

-- Users as of Jan 1, 2024 (before schema evolution)
\echo '\n=== Users as of 2024-01-01 12:00 (original schema) ==='
SELECT _id, username, email, phone_number
FROM users FOR VALID_TIME AS OF TIMESTAMP '2024-01-01T12:00:00Z'
ORDER BY _id;

-- Users as of Jan 3, 2024 (after schema evolution)
\echo '\n=== Users as of 2024-01-03 12:00 (evolved schema) ==='
SELECT _id, username, email, phone_number, verified_at
FROM users FOR VALID_TIME AS OF TIMESTAMP '2024-01-03T12:00:00Z'
ORDER BY _id;

-- =============================================================================
-- 5. JOIN QUERIES (showing relationships)
-- =============================================================================

-- Users with their profiles
\echo '\n=== Users with Profiles ==='
SELECT
    u._id as user_id,
    u.username,
    u.email,
    p.display_name,
    p.bio
FROM users u
LEFT JOIN profiles p ON u._id = p.user_id
ORDER BY u._id;

-- Active sessions with user info
\echo '\n=== Active Sessions with User Info ==='
SELECT
    s._id as session_id,
    u.username,
    s.device_type,
    s.ip_address,
    s.created_at as session_created
FROM sessions s
JOIN users u ON s.user_id = u._id
ORDER BY s.created_at DESC;

-- =============================================================================
-- 6. AGGREGATE QUERIES
-- =============================================================================

-- Count of records per table
\echo '\n=== Record Counts ==='
SELECT 'users' as table_name, COUNT(*) as current_count FROM users
UNION ALL
SELECT 'profiles', COUNT(*) FROM profiles
UNION ALL
SELECT 'sessions', COUNT(*) FROM sessions;

-- Sessions by device type
\echo '\n=== Sessions by Device Type ==='
SELECT
    COALESCE(device_type, 'unknown') as device_type,
    COUNT(*) as count
FROM sessions
GROUP BY device_type
ORDER BY count DESC;
