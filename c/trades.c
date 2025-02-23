#include <stdio.h>
#include <stdlib.h>
#include <libpq-fe.h>

#define DB_PARAMS "host=xtdb port=5432 dbname=xtdb"

void exit_on_error(PGconn *conn, PGresult *res) {
    ExecStatusType status = PQresultStatus(res);
    if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK) {
        const char *severity = PQresultErrorField(res, PG_DIAG_SEVERITY);
        const char *sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);
        const char *message_primary = PQresultErrorField(res, PG_DIAG_MESSAGE_PRIMARY);
        const char *message_detail = PQresultErrorField(res, PG_DIAG_MESSAGE_DETAIL);
        const char *message_hint = PQresultErrorField(res, PG_DIAG_MESSAGE_HINT);

        fprintf(stderr, "PostgreSQL Error [%s]: %s (SQLSTATE %s)\n",
                severity ? severity : "UNKNOWN",
                message_primary ? message_primary : "No primary message",
                sqlstate ? sqlstate : "N/A");

        if (message_detail)
            fprintf(stderr, "DETAIL: %s\n", message_detail);
        if (message_hint)
            fprintf(stderr, "HINT: %s\n", message_hint);

        PQclear(res);
        PQfinish(conn);
        exit(1);
    }
}

void insert_trades(PGconn *conn) {
    const char *query = "INSERT INTO trades (_id, name, quantity, info) VALUES ($1, $2, $3, $4)";
    const char *params[4];
    Oid paramTypes[4] = {23, 25, 23, 3802}; // int4, text, int4, jsonb

    struct {
        int id;
        char *name;
        int quantity;
        char *json_info;
    } trades[] = {
        {1, "Trade1", 1001, "{\"some_nested\": [\"json\", 42, {\"data\": [\"hello\"]}]}"},
        {2, "Trade2", 15, "2"},
        {3, "Trade3", 200, "3"}
    };

    for (size_t i = 0; i < sizeof(trades)/sizeof(trades[0]); ++i) {
        char id_str[12], quantity_str[12];
        snprintf(id_str, sizeof(id_str), "%d", trades[i].id);
        snprintf(quantity_str, sizeof(quantity_str), "%d", trades[i].quantity);

        params[0] = id_str;
        params[1] = trades[i].name;
        params[2] = quantity_str;
        params[3] = trades[i].json_info;

        PGresult *res = PQexecParams(conn, query, 4, paramTypes, params, NULL, NULL, 0);
        exit_on_error(conn, res);
        PQclear(res);
    }

    printf("Trades inserted successfully\n");
}

void get_trades_over(PGconn *conn, int quantity_threshold) {
    const char *query = "SELECT _id, name, quantity, info FROM trades WHERE quantity > $1";
    char quantity_str[12];
    snprintf(quantity_str, sizeof(quantity_str), "%d", quantity_threshold);

    const char *params[1] = { quantity_str };
    Oid paramTypes[1] = {23}; // int4
    PGresult *res = PQexecParams(conn, query, 1, paramTypes, params, NULL, NULL, 0);
    exit_on_error(conn, res);

    int rows = PQntuples(res);
    for (int i = 0; i < rows; ++i) {
        printf("Trade: ID=%s, Name=%s, Quantity=%s, Info=%s\n",
               PQgetvalue(res, i, 0),
               PQgetvalue(res, i, 1),
               PQgetvalue(res, i, 2),
               PQgetvalue(res, i, 3));
    }

    PQclear(res);
}

int main(void) {
    PGconn *conn = PQconnectdb(DB_PARAMS);
    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "Connection error: %s\n", PQerrorMessage(conn));
        PQfinish(conn);
        return 1;
    }

    insert_trades(conn);
    get_trades_over(conn, 100);

    PQfinish(conn);
    return 0;
}

