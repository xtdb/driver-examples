#!/bin/bash

# This script demonstrates bulk loading complex data structures into XTDB using PostgreSQL's COPY command

set -e

# Connection parameters
HOST=${XTDB_HOST:-xtdb}
PORT=${XTDB_PORT:-5432}
DATABASE=${XTDB_DATABASE:-xtdb}
USER=${XTDB_USER:-xtdb}
PASSWORD=${XTDB_PASSWORD:-xtdb}

# Create initial simple records
echo "=== Creating Initial Test Record ==="
psql -h $HOST -p $PORT -d $DATABASE -U $USER << 'EOF'
-- Insert a test record to ensure table exists
INSERT INTO users RECORDS {_id: 'test', name: 'Test User'};

-- Verify insertion
SELECT _id, name FROM users WHERE _id = 'test';
EOF
echo

# Load complex transit-json data from file
echo "=== Loading Complex Transit-JSON Data from File ==="
echo "This data includes:"
echo "  - Temporal fields (Date, DateTime with timezone, NaiveDateTime)"
echo "  - Boolean and numeric fields"
echo "  - Arrays with mixed types"
echo "  - Nested maps with deeply nested structures"
echo

psql -h $HOST -p $PORT -d $DATABASE -U $USER << 'EOF'
BEGIN READ WRITE WITH (ASYNC = true);

-- Use COPY to load complex transit-json formatted data
\copy users FROM 'sample-users.transit.json' WITH (FORMAT 'transit-json')

COMMIT;
EOF
echo

# Load inline complex data using COPY FROM STDIN
echo "=== Loading Complex Data via COPY FROM STDIN ==="
psql -h $HOST -p $PORT -d $DATABASE -U $USER << 'EOF'
BEGIN READ WRITE;

-- Insert complex record using COPY FROM STDIN with all data types
COPY users FROM STDIN WITH (FORMAT 'transit-json');
["^ ","~:xt/id","diana","~:name","Diana","~:created_date","~t2024-01-25","~:last_login","~t2024-01-25T16:20:15.555Z","~:next_review","~t2024-06-25T09:00:00","~:active",true,"~:age",31,"~:score",92.3,"~:tags",["admin","developer","mentor"],"~:scores",[92.3,94.1,90.5,88.7],"~:metadata",["^ ","~:role","lead_developer","~:level",8,"~:verified",true,"~:joined","~t2022-03-15","~:last_modified","~t2024-01-25T16:20:15.555Z","~:permissions",["read","write","delete","admin"],"~:settings",["^ ","~:theme","dark","~:notifications",true,"~:timezone","UTC","~:login_times",["~t2024-01-25T08:00:00Z","~t2024-01-25T12:30:00Z","~t2024-01-25T16:20:15Z"],"~:preferences",["^ ","~:language","en","~:editor","vim","~:auto_save",true]]]]
["^ ","~:xt/id","eve","~:name","Eve","~:created_date","~t2024-02-28","~:last_login","~t2024-02-28T11:45:30.123Z","~:next_review","~t2024-07-28T14:00:00","~:active",false,"~:age",45,"~:score",78.9,"~:tags",["manager","reviewer"],"~:scores",[78.9,82.3,75.5],"~:metadata",["^ ","~:role","manager","~:level",6,"~:verified",true,"~:joined","~t2021-11-20","~:last_modified","~t2024-02-28T11:45:30.123Z","~:permissions",["read","write","review"],"~:settings",["^ ","~:theme","light","~:notifications",false,"~:timezone","PST","~:login_times",["~t2024-02-28T08:00:00Z","~t2024-02-28T11:45:30Z"]]]]
\.

COMMIT;
EOF
echo

# Demonstrate complex nested data access
psql -h $HOST -p $PORT -d $DATABASE -U $USER -c "
SELECT _id, (metadata).joined, ((metadata).settings).login_times[1] as first_login
FROM users
WHERE _id = 'alice';"
echo
