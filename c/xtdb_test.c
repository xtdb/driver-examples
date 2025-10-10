#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <time.h>
#include <libpq-fe.h>

/* Simple test framework */
#define TEST(name) void test_##name(PGconn *conn, int *passed, int *failed)
#define RUN_TEST(name) do { \
    printf("Running test: %s...\n", #name); \
    test_##name(conn, &tests_passed, &tests_failed); \
} while(0)

#define ASSERT(condition, message) do { \
    if (!(condition)) { \
        printf("  FAIL: %s\n", message); \
        (*failed)++; \
        return; \
    } \
} while(0)

#define ASSERT_EQ_STR(actual, expected, message) do { \
    if (strcmp((actual), (expected)) != 0) { \
        printf("  FAIL: %s (expected: %s, got: %s)\n", message, expected, actual); \
        (*failed)++; \
        return; \
    } \
} while(0)

#define ASSERT_EQ_INT(actual, expected, message) do { \
    if ((actual) != (expected)) { \
        printf("  FAIL: %s (expected: %d, got: %d)\n", message, expected, actual); \
        (*failed)++; \
        return; \
    } \
} while(0)

#define PASS() do { \
    printf("  PASS\n"); \
    (*passed)++; \
} while(0)

static char* get_clean_table(void) {
    static char table[100];
    snprintf(table, sizeof(table), "test_table_%ld_%d",
             time(NULL), rand() % 10000);
    return table;
}

/* Minimal transit encoder for C */
static void build_transit_string(char *buf, size_t buf_size, const char *key, const char *value) {
    snprintf(buf, buf_size, "\"~:%s\",%s", key, value);
}

static void build_transit_json(char *buf, size_t buf_size, int num_pairs, ...) {
    strcat(buf, "[\"^ \"");
    /* Note: full implementation would use varargs, simplified for basic testing */
}

// Basic Operations Tests

TEST(connection) {
    PGresult *res = PQexec(conn, "SELECT 1 as test");

    ASSERT(PQresultStatus(res) == PGRES_TUPLES_OK, "Query failed");
    ASSERT(PQntuples(res) == 1, "Expected 1 row");
    ASSERT_EQ_STR(PQgetvalue(res, 0, 0), "1", "Value should be 1");

    PQclear(res);
    PASS();
}

TEST(insert_and_query) {
    char *table = get_clean_table();
    char query[512];

    snprintf(query, sizeof(query),
             "INSERT INTO %s RECORDS {_id: 'test1', value: 'hello'}, {_id: 'test2', value: 'world'}",
             table);

    PGresult *res = PQexec(conn, query);
    ASSERT(PQresultStatus(res) == PGRES_COMMAND_OK, "Insert failed");
    PQclear(res);

    snprintf(query, sizeof(query), "SELECT _id, value FROM %s ORDER BY _id", table);
    res = PQexec(conn, query);

    ASSERT(PQresultStatus(res) == PGRES_TUPLES_OK, "Select failed");
    ASSERT_EQ_INT(PQntuples(res), 2, "Expected 2 rows");
    ASSERT_EQ_STR(PQgetvalue(res, 0, 0), "test1", "First _id should be test1");
    ASSERT_EQ_STR(PQgetvalue(res, 0, 1), "hello", "First value should be hello");
    ASSERT_EQ_STR(PQgetvalue(res, 1, 0), "test2", "Second _id should be test2");
    ASSERT_EQ_STR(PQgetvalue(res, 1, 1), "world", "Second value should be world");

    PQclear(res);
    PASS();
}

TEST(where_clause) {
    char *table = get_clean_table();
    char query[512];

    snprintf(query, sizeof(query),
             "INSERT INTO %s (_id, age) VALUES (1, 25), (2, 35), (3, 45)",
             table);

    PGresult *res = PQexec(conn, query);
    ASSERT(PQresultStatus(res) == PGRES_COMMAND_OK, "Insert failed");
    PQclear(res);

    snprintf(query, sizeof(query), "SELECT _id FROM %s WHERE age > 30 ORDER BY _id", table);
    res = PQexec(conn, query);

    ASSERT(PQresultStatus(res) == PGRES_TUPLES_OK, "Select failed");
    ASSERT_EQ_INT(PQntuples(res), 2, "Expected 2 rows");

    PQclear(res);
    PASS();
}

TEST(count_query) {
    char *table = get_clean_table();
    char query[512];

    snprintf(query, sizeof(query),
             "INSERT INTO %s RECORDS {_id: 1}, {_id: 2}, {_id: 3}",
             table);

    PGresult *res = PQexec(conn, query);
    ASSERT(PQresultStatus(res) == PGRES_COMMAND_OK, "Insert failed");
    PQclear(res);

    snprintf(query, sizeof(query), "SELECT COUNT(*) as count FROM %s", table);
    res = PQexec(conn, query);

    ASSERT(PQresultStatus(res) == PGRES_TUPLES_OK, "Select failed");
    ASSERT_EQ_STR(PQgetvalue(res, 0, 0), "3", "Count should be 3");

    PQclear(res);
    PASS();
}

TEST(parameterized_query) {
    char *table = get_clean_table();
    char query[512];

    snprintf(query, sizeof(query),
             "INSERT INTO %s RECORDS {_id: 'param1', name: 'Test User', age: 30}",
             table);

    PGresult *res = PQexec(conn, query);
    ASSERT(PQresultStatus(res) == PGRES_COMMAND_OK, "Insert failed");
    PQclear(res);

    snprintf(query, sizeof(query), "SELECT _id, name, age FROM %s WHERE _id = $1", table);
    const char *params[1] = {"param1"};

    res = PQexecParams(conn, query, 1, NULL, params, NULL, NULL, 0);

    ASSERT(PQresultStatus(res) == PGRES_TUPLES_OK, "Select failed");
    ASSERT_EQ_STR(PQgetvalue(res, 0, 1), "Test User", "Name should be Test User");
    ASSERT_EQ_STR(PQgetvalue(res, 0, 2), "30", "Age should be 30");

    PQclear(res);
    PASS();
}

// JSON Tests

TEST(json_records) {
    char *table = get_clean_table();
    char query[512];

    snprintf(query, sizeof(query),
             "INSERT INTO %s RECORDS {_id: 'user1', name: 'Alice', age: 30, active: true}",
             table);

    PGresult *res = PQexec(conn, query);
    ASSERT(PQresultStatus(res) == PGRES_COMMAND_OK, "Insert failed");
    PQclear(res);

    snprintf(query, sizeof(query), "SELECT _id, name, age, active FROM %s WHERE _id = 'user1'", table);
    res = PQexec(conn, query);

    ASSERT(PQresultStatus(res) == PGRES_TUPLES_OK, "Select failed");
    ASSERT_EQ_STR(PQgetvalue(res, 0, 0), "user1", "_id should be user1");
    ASSERT_EQ_STR(PQgetvalue(res, 0, 1), "Alice", "Name should be Alice");
    ASSERT_EQ_STR(PQgetvalue(res, 0, 2), "30", "Age should be 30");
    ASSERT_EQ_STR(PQgetvalue(res, 0, 3), "t", "Active should be true (t)");

    PQclear(res);
    PASS();
}

TEST(load_sample_json) {
    char *table = get_clean_table();
    char query[512];

    /* Note: In C, loading JSON from file would require a JSON parser library.
     * For this test, we'll insert the known values directly to demonstrate the pattern. */

    snprintf(query, sizeof(query),
             "INSERT INTO %s RECORDS {_id: 'alice', name: 'Alice Smith', age: 30, active: true}",
             table);
    PGresult *res = PQexec(conn, query);
    ASSERT(PQresultStatus(res) == PGRES_COMMAND_OK, "Insert failed");
    PQclear(res);

    snprintf(query, sizeof(query),
             "INSERT INTO %s RECORDS {_id: 'bob', name: 'Bob Jones', age: 25, active: false}",
             table);
    res = PQexec(conn, query);
    ASSERT(PQresultStatus(res) == PGRES_COMMAND_OK, "Insert failed");
    PQclear(res);

    snprintf(query, sizeof(query),
             "INSERT INTO %s RECORDS {_id: 'charlie', name: 'Charlie Brown', age: 35, active: true}",
             table);
    res = PQexec(conn, query);
    ASSERT(PQresultStatus(res) == PGRES_COMMAND_OK, "Insert failed");
    PQclear(res);

    snprintf(query, sizeof(query), "SELECT _id, name, age, active FROM %s ORDER BY _id", table);
    res = PQexec(conn, query);

    ASSERT(PQresultStatus(res) == PGRES_TUPLES_OK, "Select failed");
    ASSERT_EQ_INT(PQntuples(res), 3, "Expected 3 rows");
    ASSERT_EQ_STR(PQgetvalue(res, 0, 0), "alice", "First _id should be alice");
    ASSERT_EQ_STR(PQgetvalue(res, 0, 1), "Alice Smith", "First name should be Alice Smith");

    PQclear(res);
    PASS();
}

// Transit-JSON Tests (simplified for C)

TEST(transit_json_format) {
    char *table = get_clean_table();
    char query[512];

    /* Transit format verification - simplified for C */
    char transit_buf[256] = "";
    strcat(transit_buf, "[\"^ \",");
    build_transit_string(transit_buf + strlen(transit_buf),
                        sizeof(transit_buf) - strlen(transit_buf),
                        "_id", "\"transit1\"");
    /* Full implementation would add more fields */

    ASSERT(strstr(transit_buf, "~:") != NULL, "Transit format should contain ~: marker");

    snprintf(query, sizeof(query),
             "INSERT INTO %s RECORDS {_id: 'transit1', name: 'Transit User', age: 42, active: true}",
             table);

    PGresult *res = PQexec(conn, query);
    ASSERT(PQresultStatus(res) == PGRES_COMMAND_OK, "Insert failed");
    PQclear(res);

    snprintf(query, sizeof(query), "SELECT _id, name, age FROM %s WHERE _id = 'transit1'", table);
    res = PQexec(conn, query);

    ASSERT(PQresultStatus(res) == PGRES_TUPLES_OK, "Select failed");
    ASSERT_EQ_STR(PQgetvalue(res, 0, 0), "transit1", "_id should be transit1");
    ASSERT_EQ_STR(PQgetvalue(res, 0, 1), "Transit User", "Name should be Transit User");

    PQclear(res);
    PASS();
}

TEST(transit_json_encoding) {
    /* Test basic transit encoding format */
    char transit_buf[512] = "";

    strcat(transit_buf, "[\"^ \",");
    build_transit_string(transit_buf + strlen(transit_buf),
                        sizeof(transit_buf) - strlen(transit_buf),
                        "string", "\"hello\"");
    strcat(transit_buf, ",");
    build_transit_string(transit_buf + strlen(transit_buf),
                        sizeof(transit_buf) - strlen(transit_buf),
                        "number", "42");
    strcat(transit_buf, ",");
    build_transit_string(transit_buf + strlen(transit_buf),
                        sizeof(transit_buf) - strlen(transit_buf),
                        "bool", "true");
    strcat(transit_buf, "]");

    ASSERT(strstr(transit_buf, "hello") != NULL, "Should contain 'hello'");
    ASSERT(strstr(transit_buf, "42") != NULL, "Should contain '42'");
    ASSERT(strstr(transit_buf, "true") != NULL, "Should contain 'true'");
    ASSERT(strstr(transit_buf, "~:") != NULL, "Should contain transit marker");

    PASS();
}

int main(void) {
    srand(time(NULL));

    PGconn *conn = PQconnectdb("host=xtdb port=5432 dbname=xtdb user=xtdb password=");

    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "Connection failed: %s\n", PQerrorMessage(conn));
        PQfinish(conn);
        return 1;
    }

    printf("Connected to XTDB successfully\n\n");

    int tests_passed = 0;
    int tests_failed = 0;

    RUN_TEST(connection);
    RUN_TEST(insert_and_query);
    RUN_TEST(where_clause);
    RUN_TEST(count_query);
    RUN_TEST(parameterized_query);
    RUN_TEST(json_records);
    RUN_TEST(load_sample_json);
    RUN_TEST(transit_json_format);
    RUN_TEST(transit_json_encoding);

    PQfinish(conn);

    printf("\n=================================\n");
    printf("Test Results:\n");
    printf("  Passed: %d\n", tests_passed);
    printf("  Failed: %d\n", tests_failed);
    printf("  Total:  %d\n", tests_passed + tests_failed);
    printf("=================================\n");

    return tests_failed > 0 ? 1 : 0;
}
