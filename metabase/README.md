# Metabase

Metabase is a business intelligence tool that can connect to XTDB via the PostgreSQL wire protocol.

## Setup

1. Open port 3000 from the forwarded ports in the Codespace UI
2. Enter dummy details for the initial setup:
   - First name: `a`
   - Last name: `a`
   - Email: `a@a.com`
   - Password: `passw0rd!`
3. When prompted to connect a database, select PostgreSQL and paste the connection string:
   ```
   jdbc:postgresql://xtdb:xtdb@xtdb:5432/xtdb
   ```
