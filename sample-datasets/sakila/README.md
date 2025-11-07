# Sakila - a movie rental database

If Clojure is installed then `./load.sh` should be enough to load this [Sakila](https://www.jooq.org/sakila) dataset into XTDB.

See the GitHub action workflow for exact usage.

TSV data is derived from `postgres-sakila-insert-data-using-copy.sql` @ https://github.com/jOOQ/sakila/tree/main/postgres-sakila-db

- `modified_at` columns are now `_valid_from`
- `create_date` remains a user-defined field, so is not the same as `MIN(_valid_from)`
- `<table_id>` is transformed as `_id` within XTDB
