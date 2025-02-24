#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <time.h>
#include <signal.h>
#include <stdbool.h>
#include <libpq-fe.h>
#include <getopt.h>

/* Configuration */
#define MAX_INT_STR_LEN 32
#define MAX_QUERY_LEN 1024
#define DEFAULT_DB_PARAMS "host=localhost port=5432 dbname=xtdb"
#define LOG_BUF_SIZE 2048

/* Exit codes */
#define EXIT_SUCCESS 0
#define EXIT_DB_CONNECTION_ERROR 1
#define EXIT_QUERY_ERROR 2
#define EXIT_BUFFER_OVERFLOW 3
#define EXIT_MEMORY_ERROR 4
#define EXIT_INVALID_ARGS 5

/* Log levels */
typedef enum
{
    LOG_ERROR,
    LOG_WARN,
    LOG_INFO,
    LOG_DEBUG
} log_level_t;

static log_level_t current_log_level = LOG_INFO;
static PGconn *global_conn = NULL;
static volatile sig_atomic_t shutdown_requested = 0;

/* Function declarations */
void cleanup(void);
void signal_handler(int signum);
void log_message(log_level_t level, const char *format, ...);
void handle_db_error(PGconn *conn, PGresult *res, const char *context);
bool connect_db(const char *connection_string);
void disconnect_db(void);
bool begin_transaction(PGconn *conn);
bool commit_transaction(PGconn *conn);
bool rollback_transaction(PGconn *conn);

/* Trade operations */
typedef struct
{
    int id;
    char *name;
    int quantity;
    char *json_info;
} trade_info;

trade_info *trade_create(int id, const char *name, int quantity, const char *json_info);
void trade_destroy(trade_info *trade);
bool validate_trade(const trade_info *trade);
bool insert_trade(PGconn *conn, const trade_info *trade);
bool insert_trades_batch(PGconn *conn, trade_info **trades, size_t count);
void get_trades_over_quantity(PGconn *conn, int quantity_threshold);

void print_usage(const char *program_name)
{
    printf("Usage: %s [OPTIONS]\n", program_name);
    printf("Options:\n");
    printf("  -h, --host HOST      Database host (default: localhost)\n");
    printf("  -p, --port PORT      Database port (default: 5432)\n");
    printf("  -d, --dbname NAME    Database name (default: xtdb)\n");
    printf("  -u, --user USER      Database user\n");
    printf("  -w, --password PASS  Database password\n");
    printf("  -v, --verbose        Increase verbosity\n");
    printf("  -q, --quiet          Decrease verbosity\n");
    printf("  -?, --help           Display this help and exit\n");
}

void log_message(log_level_t level, const char *format, ...)
{
    if (level > current_log_level)
    {
        return;
    }

    time_t now;
    time(&now);
    char timestamp[26];
    ctime_r(&now, timestamp);
    timestamp[24] = '\0'; /* Remove newline from timestamp */

    const char *level_str;
    switch (level)
    {
    case LOG_ERROR:
        level_str = "ERROR";
        break;
    case LOG_WARN:
        level_str = "WARN";
        break;
    case LOG_INFO:
        level_str = "INFO";
        break;
    case LOG_DEBUG:
        level_str = "DEBUG";
        break;
    default:
        level_str = "UNKNOWN";
    }

    /* Format the log message with timestamp and level */
    char log_buf[LOG_BUF_SIZE];
    int header_len = snprintf(log_buf, LOG_BUF_SIZE, "[%s] [%s] ", timestamp, level_str);
    if (header_len < 0 || header_len >= LOG_BUF_SIZE)
    {
        fprintf(stderr, "[ERROR] Log buffer overflow\n");
        return;
    }

    va_list args;
    va_start(args, format);
    int msg_len = vsnprintf(log_buf + header_len, LOG_BUF_SIZE - header_len, format, args);
    va_end(args);

    if (msg_len < 0 || header_len + msg_len >= LOG_BUF_SIZE)
    {
        fprintf(stderr, "[ERROR] Log message truncated\n");
    }

    FILE *output = (level == LOG_ERROR || level == LOG_WARN) ? stderr : stdout;
    fprintf(output, "%s", log_buf);
    fflush(output);
}

void handle_db_error(PGconn *conn, PGresult *res, const char *context)
{
    /* Silence unused parameter warning */
    (void)conn;

    const char *severity = PQresultErrorField(res, PG_DIAG_SEVERITY);
    const char *sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);
    const char *message_primary = PQresultErrorField(res, PG_DIAG_MESSAGE_PRIMARY);
    const char *message_detail = PQresultErrorField(res, PG_DIAG_MESSAGE_DETAIL);

    log_message(LOG_ERROR, "PostgreSQL Error in %s [%s]: %s (SQLSTATE %s)\n",
                context,
                severity ? severity : "UNKNOWN",
                message_primary ? message_primary : "No primary message",
                sqlstate ? sqlstate : "N/A");

    if (message_detail)
    {
        log_message(LOG_ERROR, "Detail: %s\n", message_detail);
    }

    PQclear(res);
}

void cleanup(void)
{
    log_message(LOG_DEBUG, "Performing cleanup\n");
    disconnect_db();
}

void signal_handler(int signum)
{
    log_message(LOG_INFO, "Received signal %d, marking for shutdown...\n", signum);
    shutdown_requested = 1;
}

bool connect_db(const char *connection_string)
{
    if (global_conn)
    {
        log_message(LOG_WARN, "Already connected to database, disconnecting first\n");
        disconnect_db();
    }

    log_message(LOG_INFO, "Connecting to database...\n");
    global_conn = PQconnectdb(connection_string);

    if (PQstatus(global_conn) != CONNECTION_OK)
    {
        log_message(LOG_ERROR, "Connection error: %s\n", PQerrorMessage(global_conn));
        PQfinish(global_conn);
        global_conn = NULL;
        return false;
    }

    log_message(LOG_INFO, "Connected to database successfully\n");
    return true;
}

void disconnect_db(void)
{
    if (global_conn)
    {
        log_message(LOG_INFO, "Disconnecting from database\n");
        PQfinish(global_conn);
        global_conn = NULL;
    }
}

bool begin_transaction(PGconn *conn)
{
    if (!conn || PQstatus(conn) != CONNECTION_OK)
    {
        log_message(LOG_ERROR, "Cannot begin transaction: Invalid connection\n");
        return false;
    }

    PGresult *res = PQexec(conn, "BEGIN");
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        handle_db_error(conn, res, "begin_transaction");
        PQclear(res);
        return false;
    }

    PQclear(res);
    log_message(LOG_DEBUG, "Transaction started\n");
    return true;
}

bool commit_transaction(PGconn *conn)
{
    if (!conn || PQstatus(conn) != CONNECTION_OK)
    {
        log_message(LOG_ERROR, "Cannot commit transaction: Invalid connection\n");
        return false;
    }

    PGresult *res = PQexec(conn, "COMMIT");
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        handle_db_error(conn, res, "commit_transaction");
        PQclear(res);
        return false;
    }

    PQclear(res);
    log_message(LOG_DEBUG, "Transaction committed\n");
    return true;
}

bool rollback_transaction(PGconn *conn)
{
    if (!conn || PQstatus(conn) != CONNECTION_OK)
    {
        log_message(LOG_ERROR, "Cannot rollback transaction: Invalid connection\n");
        return false;
    }

    PGresult *res = PQexec(conn, "ROLLBACK");
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        handle_db_error(conn, res, "rollback_transaction");
        PQclear(res);
        return false;
    }

    PQclear(res);
    log_message(LOG_DEBUG, "Transaction rolled back\n");
    return true;
}

trade_info *trade_create(int id, const char *name, int quantity, const char *json_info)
{
    if (!name || !json_info)
    {
        log_message(LOG_ERROR, "Cannot create trade: NULL parameters\n");
        return NULL;
    }

    trade_info *trade = malloc(sizeof(trade_info));
    if (!trade)
    {
        log_message(LOG_ERROR, "Memory allocation failed for trade\n");
        return NULL;
    }

    trade->id = id;
    trade->quantity = quantity;

    trade->name = strdup(name);
    if (!trade->name)
    {
        log_message(LOG_ERROR, "Memory allocation failed for trade name\n");
        free(trade);
        return NULL;
    }

    trade->json_info = strdup(json_info);
    if (!trade->json_info)
    {
        log_message(LOG_ERROR, "Memory allocation failed for trade JSON info\n");
        free(trade->name);
        free(trade);
        return NULL;
    }

    return trade;
}

void trade_destroy(trade_info *trade)
{
    if (trade)
    {
        free(trade->name);
        free(trade->json_info);
        free(trade);
    }
}

bool validate_trade(const trade_info *trade)
{
    if (!trade || !trade->name || !trade->json_info)
    {
        log_message(LOG_ERROR, "Invalid trade data: NULL values detected\n");
        return false;
    }

    if (trade->quantity <= 0)
    {
        log_message(LOG_ERROR, "Invalid trade quantity: %d\n", trade->quantity);
        return false;
    }

    return true;
}

bool insert_trade(PGconn *conn, const trade_info *trade)
{
    if (!conn || PQstatus(conn) != CONNECTION_OK)
    {
        log_message(LOG_ERROR, "Cannot insert trade: Invalid connection\n");
        return false;
    }

    if (!validate_trade(trade))
    {
        return false;
    }

    char id_str[MAX_INT_STR_LEN];
    char quantity_str[MAX_INT_STR_LEN];

    if (snprintf(id_str, sizeof(id_str), "%d", trade->id) >= (int)sizeof(id_str) ||
        snprintf(quantity_str, sizeof(quantity_str), "%d", trade->quantity) >= (int)sizeof(quantity_str))
    {
        log_message(LOG_ERROR, "Buffer overflow in number conversion\n");
        return false;
    }

    const char *query = "INSERT INTO trades (_id, name, quantity, info) VALUES ($1, $2, $3, $4)";
    const char *params[4] = {id_str, trade->name, quantity_str, trade->json_info};
    Oid paramTypes[4] = {23, 25, 23, 3802}; /* int4, text, int4, jsonb */

    PGresult *res = PQexecParams(conn, query, 4, paramTypes, params, NULL, NULL, 0);

    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        handle_db_error(conn, res, "insert_trade");
        PQclear(res);
        return false;
    }

    PQclear(res);
    log_message(LOG_DEBUG, "Inserted trade ID %d: %s, Quantity: %d\n",
                trade->id, trade->name, trade->quantity);
    return true;
}

bool insert_trades_batch(PGconn *conn, trade_info **trades, size_t count)
{
    if (!conn || PQstatus(conn) != CONNECTION_OK)
    {
        log_message(LOG_ERROR, "Cannot insert trades batch: Invalid connection\n");
        return false;
    }

    if (!trades || count == 0)
    {
        log_message(LOG_ERROR, "Cannot insert trades batch: Empty trades array\n");
        return false;
    }

    bool success = begin_transaction(conn);
    if (!success)
    {
        return false;
    }

    for (size_t i = 0; i < count; ++i)
    {
        if (shutdown_requested)
        {
            log_message(LOG_WARN, "Shutdown requested, aborting batch insertion\n");
            rollback_transaction(conn);
            return false;
        }

        if (!insert_trade(conn, trades[i]))
        {
            log_message(LOG_ERROR, "Failed to insert trade %zu in batch, rolling back\n", i + 1);
            rollback_transaction(conn);
            return false;
        }
    }

    return commit_transaction(conn);
}

void get_trades_over_quantity(PGconn *conn, int quantity_threshold)
{
    if (!conn || PQstatus(conn) != CONNECTION_OK)
    {
        log_message(LOG_ERROR, "Cannot query trades: Invalid connection\n");
        return;
    }

    if (quantity_threshold < 0)
    {
        log_message(LOG_ERROR, "Invalid quantity threshold: %d\n", quantity_threshold);
        return;
    }

    char quantity_str[MAX_INT_STR_LEN];
    if (snprintf(quantity_str, sizeof(quantity_str), "%d", quantity_threshold) >= (int)sizeof(quantity_str))
    {
        log_message(LOG_ERROR, "Buffer overflow in quantity threshold conversion\n");
        return;
    }

    const char *query = "SELECT _id, name, quantity, info FROM trades WHERE quantity > $1";
    const char *params[1] = {quantity_str};
    Oid paramTypes[1] = {23}; /* int4 */

    PGresult *res = PQexecParams(conn, query, 1, paramTypes, params, NULL, NULL, 0);

    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        handle_db_error(conn, res, "get_trades_over_quantity");
        PQclear(res);
        return;
    }

    int rows = PQntuples(res);
    log_message(LOG_INFO, "Found %d trades over quantity %d:\n", rows, quantity_threshold);

    for (int i = 0; i < rows; ++i)
    {
        log_message(LOG_INFO, "Trade: ID=%s, Name=%s, Quantity=%s, Info=%s\n",
                    PQgetvalue(res, i, 0),
                    PQgetvalue(res, i, 1),
                    PQgetvalue(res, i, 2),
                    PQgetvalue(res, i, 3));
    }

    PQclear(res);
}

int main(int argc, char *argv[])
{
    int exit_code = EXIT_SUCCESS;
    char *host = NULL;
    char *port = NULL;
    char *dbname = NULL;
    char *user = NULL;
    char *password = NULL;

    /* Register cleanup handlers */
    atexit(cleanup);
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    /* Process command-line arguments */
    static struct option long_options[] = {
        {"host", required_argument, 0, 'h'},
        {"port", required_argument, 0, 'p'},
        {"dbname", required_argument, 0, 'd'},
        {"user", required_argument, 0, 'u'},
        {"password", required_argument, 0, 'w'},
        {"verbose", no_argument, 0, 'v'},
        {"quiet", no_argument, 0, 'q'},
        {"help", no_argument, 0, '?'},
        {0, 0, 0, 0}};

    int option_index = 0;
    int c;
    while ((c = getopt_long(argc, argv, "h:p:d:u:w:vq?", long_options, &option_index)) != -1)
    {
        switch (c)
        {
        case 'h':
            host = optarg;
            break;
        case 'p':
            port = optarg;
            break;
        case 'd':
            dbname = optarg;
            break;
        case 'u':
            user = optarg;
            break;
        case 'w':
            password = optarg;
            break;
        case 'v':
            if (current_log_level < LOG_DEBUG)
            {
                current_log_level++;
            }
            break;
        case 'q':
            if (current_log_level > LOG_ERROR)
            {
                current_log_level--;
            }
            break;
        case '?':
            print_usage(argv[0]);
            return EXIT_SUCCESS;
        default:
            print_usage(argv[0]);
            return EXIT_INVALID_ARGS;
        }
    }

    /* Build connection string */
    char connection_string[1024] = "";
    size_t pos = 0;

    if (host)
    {
        pos += snprintf(connection_string + pos, sizeof(connection_string) - pos,
                        "host=%s ", host);
    }

    if (port)
    {
        pos += snprintf(connection_string + pos, sizeof(connection_string) - pos,
                        "port=%s ", port);
    }

    if (dbname)
    {
        pos += snprintf(connection_string + pos, sizeof(connection_string) - pos,
                        "dbname=%s ", dbname);
    }

    if (user)
    {
        pos += snprintf(connection_string + pos, sizeof(connection_string) - pos,
                        "user=%s ", user);
    }

    if (password)
    {
        pos += snprintf(connection_string + pos, sizeof(connection_string) - pos,
                        "password=%s ", password);
    }

    if (pos == 0)
    {
        strncpy(connection_string, DEFAULT_DB_PARAMS, sizeof(connection_string) - 1);
        connection_string[sizeof(connection_string) - 1] = '\0';
    }

    if (!connect_db(connection_string))
    {
        return EXIT_DB_CONNECTION_ERROR;
    }

    /* Create sample trade data */
    trade_info *trades[3];
    trades[0] = trade_create(1, "Trade1", 1001, "{\"some_nested\": [\"json\", 42, {\"data\": [\"hello\"]}]}");
    trades[1] = trade_create(2, "Trade2", 15, "{\"value\": 2}");
    trades[2] = trade_create(3, "Trade3", 200, "{\"value\": 3}");

    /* Check if all trades were created successfully */
    bool all_trades_valid = true;
    for (int i = 0; i < 3; i++)
    {
        if (!trades[i])
        {
            all_trades_valid = false;
            break;
        }
    }

    if (all_trades_valid)
    {
        /* Insert trades in a batch (transactional) */
        if (insert_trades_batch(global_conn, trades, 3))
        {
            log_message(LOG_INFO, "Trades inserted successfully in batch\n");

            /* Query trades */
            get_trades_over_quantity(global_conn, 100);
        }
        else
        {
            log_message(LOG_ERROR, "Errors occurred while inserting trades batch\n");
            exit_code = EXIT_QUERY_ERROR;
        }
    }
    else
    {
        log_message(LOG_ERROR, "Failed to create one or more trades\n");
        exit_code = EXIT_MEMORY_ERROR;
    }

    /* Free trade data */
    for (int i = 0; i < 3; i++)
    {
        trade_destroy(trades[i]);
    }

    return exit_code;
}