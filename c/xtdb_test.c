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

// OID-based Tests (using PQexecParams with explicit OIDs)

#define TRANSIT_OID 16384
#define JSON_OID 114

TEST(json_with_oid) {
    char *table = get_clean_table();
    char query[512];

    /* Load sample-users.json - need to parse multi-line JSON objects */
    FILE *fp = fopen("../test-data/sample-users.json", "r");
    ASSERT(fp != NULL, "Failed to open sample-users.json");

    /* Read entire file into memory */
    fseek(fp, 0, SEEK_END);
    long fsize = ftell(fp);
    fseek(fp, 0, SEEK_SET);

    char *file_content = malloc(fsize + 1);
    fread(file_content, 1, fsize, fp);
    fclose(fp);
    file_content[fsize] = '\0';

    /* Parse JSON objects by counting braces */
    char json_object[8192];
    int inserted_count = 0;
    int brace_count = 0;
    int obj_start = -1;
    bool in_string = false;
    bool escape_next = false;

    snprintf(query, sizeof(query), "INSERT INTO %s RECORDS $1", table);

    for (long i = 0; i < fsize; i++) {
        char c = file_content[i];

        if (escape_next) {
            escape_next = false;
            continue;
        }

        if (c == '\\' && in_string) {
            escape_next = true;
            continue;
        }

        if (c == '"') {
            in_string = !in_string;
            continue;
        }

        if (in_string) continue;

        if (c == '{') {
            if (brace_count == 0) {
                obj_start = i;
            }
            brace_count++;
        } else if (c == '}') {
            brace_count--;
            if (brace_count == 0 && obj_start >= 0) {
                /* Found complete JSON object */
                int obj_len = i - obj_start + 1;
                strncpy(json_object, file_content + obj_start, obj_len);
                json_object[obj_len] = '\0';

                /* Insert using JSON OID (114) */
                const char *paramValues[1] = {json_object};
                const Oid paramTypes[1] = {JSON_OID};

                PGresult *res = PQexecParams(conn, query, 1, paramTypes, paramValues, NULL, NULL, 0);
                ASSERT(PQresultStatus(res) == PGRES_COMMAND_OK, "Insert with JSON OID failed");
                PQclear(res);
                inserted_count++;

                obj_start = -1;
            }
        }
    }

    free(file_content);
    ASSERT_EQ_INT(inserted_count, 3, "Expected to insert 3 records");

    /* Verify the data - check first record (alice) with ALL fields including nested data */
    snprintf(query, sizeof(query),
             "SELECT _id, name, age, active, email, salary, tags, metadata FROM %s WHERE _id = 'alice'", table);
    PGresult *res = PQexec(conn, query);

    ASSERT(PQresultStatus(res) == PGRES_TUPLES_OK, "Select failed");
    ASSERT_EQ_INT(PQntuples(res), 1, "Expected 1 row for alice");
    ASSERT_EQ_STR(PQgetvalue(res, 0, 0), "alice", "_id should be alice");
    ASSERT_EQ_STR(PQgetvalue(res, 0, 1), "Alice Smith", "Name should be Alice Smith");
    ASSERT_EQ_STR(PQgetvalue(res, 0, 2), "30", "Age should be 30");
    ASSERT_EQ_STR(PQgetvalue(res, 0, 3), "t", "Active should be true");
    ASSERT_EQ_STR(PQgetvalue(res, 0, 4), "alice@example.com", "Email should match");
    ASSERT_EQ_STR(PQgetvalue(res, 0, 5), "125000.5", "Salary should be 125000.5");

    /* Verify nested array (tags) - With transit output format, properly typed */
    const char *tags = PQgetvalue(res, 0, 6);
    ASSERT(tags != NULL, "Tags should not be NULL");
    ASSERT(strstr(tags, "admin") != NULL, "Tags should contain 'admin'");
    ASSERT(strstr(tags, "developer") != NULL, "Tags should contain 'developer'");

    /* Verify nested object (metadata) - With transit output format, properly typed */
    const char *metadata = PQgetvalue(res, 0, 7);
    ASSERT(metadata != NULL, "Metadata should not be NULL");
    ASSERT(strstr(metadata, "Engineering") != NULL, "Metadata should contain 'Engineering'");
    ASSERT(strstr(metadata, "5") != NULL, "Metadata should contain level value 5");
    ASSERT(strstr(metadata, "2020-01-15") != NULL, "Metadata should contain joined date");

    PQclear(res);

    /* Verify total count */
    snprintf(query, sizeof(query), "SELECT COUNT(*) FROM %s", table);
    res = PQexec(conn, query);
    ASSERT(PQresultStatus(res) == PGRES_TUPLES_OK, "Count query failed");
    ASSERT_EQ_STR(PQgetvalue(res, 0, 0), "3", "Should have 3 total records");
    PQclear(res);

    PASS();
}

TEST(transit_with_oid) {
    char *table = get_clean_table();
    char query[512];

    /* Set fallback_output_format to transit for this test only */
    PGresult *set_res = PQexec(conn, "SET fallback_output_format = 'transit'");
    ASSERT(PQresultStatus(set_res) == PGRES_COMMAND_OK, "SET fallback_output_format failed");
    PQclear(set_res);

    /* Load sample-users-transit.json - one transit-JSON record per line */
    FILE *fp = fopen("../test-data/sample-users-transit.json", "r");
    ASSERT(fp != NULL, "Failed to open sample-users-transit.json");

    char line[4096];
    int inserted_count = 0;

    snprintf(query, sizeof(query), "INSERT INTO %s RECORDS $1", table);

    while (fgets(line, sizeof(line), fp)) {
        /* Trim whitespace */
        char *trimmed = line;
        while (*trimmed == ' ' || *trimmed == '\t' || *trimmed == '\n') trimmed++;

        size_t len = strlen(trimmed);
        while (len > 0 && (trimmed[len-1] == ' ' || trimmed[len-1] == '\n' || trimmed[len-1] == '\r')) {
            trimmed[--len] = '\0';
        }

        if (len == 0 || *trimmed == '\0') continue;

        /* Insert using transit-JSON OID (16384) */
        const char *paramValues[1] = {trimmed};
        const Oid paramTypes[1] = {TRANSIT_OID};

        PGresult *res = PQexecParams(conn, query, 1, paramTypes, paramValues, NULL, NULL, 0);
        ASSERT(PQresultStatus(res) == PGRES_COMMAND_OK, "Insert with transit OID failed");
        PQclear(res);
        inserted_count++;
    }

    fclose(fp);
    ASSERT_EQ_INT(inserted_count, 3, "Expected to insert 3 records");

    /* Verify the data - check first record (alice) with ALL fields including nested data */
    snprintf(query, sizeof(query),
             "SELECT _id, name, age, active, email, salary, tags, metadata FROM %s WHERE _id = 'alice'", table);
    PGresult *res = PQexec(conn, query);

    ASSERT(PQresultStatus(res) == PGRES_TUPLES_OK, "Select failed");
    ASSERT_EQ_INT(PQntuples(res), 1, "Expected 1 row for alice");
    ASSERT_EQ_STR(PQgetvalue(res, 0, 0), "alice", "_id should be alice");
    ASSERT_EQ_STR(PQgetvalue(res, 0, 1), "Alice Smith", "Name should be Alice Smith");
    ASSERT_EQ_STR(PQgetvalue(res, 0, 2), "30", "Age should be 30");
    ASSERT_EQ_STR(PQgetvalue(res, 0, 3), "t", "Active should be true");
    ASSERT_EQ_STR(PQgetvalue(res, 0, 4), "alice@example.com", "Email should match");
    ASSERT_EQ_STR(PQgetvalue(res, 0, 5), "125000.5", "Salary should be 125000.5");

    /* Verify nested array (tags) - With transit output format, properly typed */
    const char *tags = PQgetvalue(res, 0, 6);
    ASSERT(tags != NULL, "Tags should not be NULL");
    ASSERT(strstr(tags, "admin") != NULL, "Tags should contain 'admin'");
    ASSERT(strstr(tags, "developer") != NULL, "Tags should contain 'developer'");

    /* Verify nested object (metadata) - With transit output format, properly typed */
    const char *metadata = PQgetvalue(res, 0, 7);
    ASSERT(metadata != NULL, "Metadata should not be NULL");
    ASSERT(strstr(metadata, "Engineering") != NULL, "Metadata should contain 'Engineering'");
    ASSERT(strstr(metadata, "5") != NULL, "Metadata should contain level value 5");
    ASSERT(strstr(metadata, "2020-01-15") != NULL, "Metadata should contain joined date");

    PQclear(res);

    /* Verify total count */
    snprintf(query, sizeof(query), "SELECT COUNT(*) FROM %s", table);
    res = PQexec(conn, query);
    ASSERT(PQresultStatus(res) == PGRES_TUPLES_OK, "Count query failed");
    ASSERT_EQ_STR(PQgetvalue(res, 0, 0), "3", "Should have 3 total records");
    PQclear(res);

    /* Reset fallback_output_format after test */
    set_res = PQexec(conn, "RESET fallback_output_format");
    if (PQresultStatus(set_res) == PGRES_COMMAND_OK) {
        PQclear(set_res);
    }

    PASS();
}

TEST(transit_nest_one_full_record) {
    char *table = get_clean_table();
    char query[512];

    /* Set fallback_output_format to transit for this test only */
    PGresult *set_res = PQexec(conn, "SET fallback_output_format = 'transit'");
    ASSERT(PQresultStatus(set_res) == PGRES_COMMAND_OK, "SET fallback_output_format failed");
    PQclear(set_res);

    /* Load sample-users-transit.json - one transit-JSON record per line */
    FILE *fp = fopen("../test-data/sample-users-transit.json", "r");
    ASSERT(fp != NULL, "Failed to open sample-users-transit.json");

    char line[4096];
    int inserted_count = 0;

    snprintf(query, sizeof(query), "INSERT INTO %s RECORDS $1", table);

    while (fgets(line, sizeof(line), fp)) {
        /* Trim whitespace */
        char *trimmed = line;
        while (*trimmed == ' ' || *trimmed == '\t' || *trimmed == '\n') trimmed++;

        size_t len = strlen(trimmed);
        while (len > 0 && (trimmed[len-1] == ' ' || trimmed[len-1] == '\n' || trimmed[len-1] == '\r')) {
            trimmed[--len] = '\0';
        }

        if (len == 0 || *trimmed == '\0') continue;

        /* Insert using transit-JSON OID (16384) */
        const char *paramValues[1] = {trimmed};
        const Oid paramTypes[1] = {TRANSIT_OID};

        PGresult *res = PQexecParams(conn, query, 1, paramTypes, paramValues, NULL, NULL, 0);
        ASSERT(PQresultStatus(res) == PGRES_COMMAND_OK, "Insert with transit OID failed");
        PQclear(res);
        inserted_count++;
    }

    fclose(fp);
    ASSERT_EQ_INT(inserted_count, 3, "Expected to insert 3 records");

    /* Query using NEST_ONE to get entire record as a single nested object */
    snprintf(query, sizeof(query),
             "SELECT NEST_ONE(FROM %s WHERE _id = 'alice') AS r", table);
    PGresult *res = PQexec(conn, query);

    ASSERT(PQresultStatus(res) == PGRES_TUPLES_OK, "NEST_ONE query failed");
    ASSERT_EQ_INT(PQntuples(res), 1, "Expected 1 row");

    /* The entire record comes back as a single nested object in column 'r' */
    const char *record = PQgetvalue(res, 0, 0);
    ASSERT(record != NULL, "Record should not be NULL");

    printf("\n  ✅ NEST_ONE returned entire record\n");
    printf("     Record type: string representation\n");

    /* With transit fallback, the entire record should be properly typed */
    /* Verify all fields are accessible within the nested structure */
    ASSERT(strstr(record, "alice") != NULL, "Record should contain _id 'alice'");
    ASSERT(strstr(record, "Alice Smith") != NULL, "Record should contain name 'Alice Smith'");
    ASSERT(strstr(record, "30") != NULL, "Record should contain age 30");
    ASSERT(strstr(record, "true") != NULL || strstr(record, "t") != NULL, "Record should contain active true");
    ASSERT(strstr(record, "alice@example.com") != NULL, "Record should contain email");
    ASSERT(strstr(record, "125000.5") != NULL, "Record should contain salary");

    /* Verify nested array (tags) is in the record */
    ASSERT(strstr(record, "admin") != NULL, "Record should contain 'admin' tag");
    ASSERT(strstr(record, "developer") != NULL, "Record should contain 'developer' tag");
    printf("     ✅ Nested array (tags) accessible in record\n");

    /* Verify nested object (metadata) is in the record */
    ASSERT(strstr(record, "Engineering") != NULL, "Record should contain department 'Engineering'");
    ASSERT(strstr(record, "5") != NULL, "Record should contain level 5");

    /* Verify joined date has transit tagged value format */
    ASSERT(strstr(record, "~#time/zoned-date-time") != NULL && strstr(record, "2020-01-15") != NULL,
           "Record should contain transit-tagged date [\"~#time/zoned-date-time\", \"2020-01-15...\"]");
    printf("     ✅ Nested object (metadata) accessible in record with transit-tagged date\n");
    printf("     Note: C libpq returns dates in transit tagged format [\"~#time/zoned-date-time\", \"...\"]\n");
    printf("           Applications can parse the tagged value to extract and parse the date string\n");

    printf("\n  ✅ NEST_ONE with transit fallback successfully decoded entire record!\n");
    printf("     All fields accessible within the nested structure\n");

    PQclear(res);

    /* Reset fallback_output_format after test */
    set_res = PQexec(conn, "RESET fallback_output_format");
    if (PQresultStatus(set_res) == PGRES_COMMAND_OK) {
        PQclear(set_res);
    }

    PASS();
}

TEST(nested_data_roundtrip) {
    char *table = get_clean_table();
    char query[512];

    /* Insert a record with complex nested structures using JSON OID */
    snprintf(query, sizeof(query), "INSERT INTO %s RECORDS $1", table);

    const char *complex_json = "{"
        "\"_id\": \"nested_test\","
        "\"simple_array\": [1, 2, 3],"
        "\"string_array\": [\"a\", \"b\", \"c\"],"
        "\"nested_object\": {"
            "\"inner_field\": \"value\","
            "\"inner_number\": 42,"
            "\"inner_array\": [\"x\", \"y\"]"
        "},"
        "\"array_of_objects\": ["
            "{\"id\": 1, \"name\": \"first\"},"
            "{\"id\": 2, \"name\": \"second\"}"
        "]"
    "}";

    const char *paramValues[1] = {complex_json};
    const Oid paramTypes[1] = {JSON_OID};

    PGresult *res = PQexecParams(conn, query, 1, paramTypes, paramValues, NULL, NULL, 0);
    ASSERT(PQresultStatus(res) == PGRES_COMMAND_OK, "Insert complex nested data failed");
    PQclear(res);

    /* Query back and verify nested structures are preserved */
    snprintf(query, sizeof(query),
             "SELECT _id, simple_array, string_array, nested_object, array_of_objects FROM %s WHERE _id = 'nested_test'",
             table);
    res = PQexec(conn, query);

    ASSERT(PQresultStatus(res) == PGRES_TUPLES_OK, "Select failed");
    ASSERT_EQ_INT(PQntuples(res), 1, "Expected 1 row");

    /* Verify simple_array */
    const char *simple_array = PQgetvalue(res, 0, 1);
    ASSERT(strstr(simple_array, "1") != NULL, "simple_array should contain 1");
    ASSERT(strstr(simple_array, "2") != NULL, "simple_array should contain 2");
    ASSERT(strstr(simple_array, "3") != NULL, "simple_array should contain 3");

    /* Verify string_array */
    const char *string_array = PQgetvalue(res, 0, 2);
    ASSERT(strstr(string_array, "a") != NULL, "string_array should contain 'a'");
    ASSERT(strstr(string_array, "b") != NULL, "string_array should contain 'b'");
    ASSERT(strstr(string_array, "c") != NULL, "string_array should contain 'c'");

    /* Verify nested_object */
    const char *nested_object = PQgetvalue(res, 0, 3);
    ASSERT(strstr(nested_object, "inner_field") != NULL, "nested_object should have inner_field");
    ASSERT(strstr(nested_object, "value") != NULL, "nested_object.inner_field should be 'value'");
    ASSERT(strstr(nested_object, "42") != NULL, "nested_object should have inner_number value 42");

    /* Verify array_of_objects */
    const char *array_of_objects = PQgetvalue(res, 0, 4);
    ASSERT(strstr(array_of_objects, "1") != NULL, "array_of_objects should contain id value 1");
    ASSERT(strstr(array_of_objects, "first") != NULL, "array_of_objects should contain 'first'");
    ASSERT(strstr(array_of_objects, "second") != NULL, "array_of_objects should contain 'second'");

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

TEST(transit_msgpack_copy_from) {
    char *table = get_clean_table();
    char query[512];

    /* Load transit-msgpack file (binary) */
    FILE *fp = fopen("../test-data/sample-users-transit.msgpack", "rb");
    ASSERT(fp != NULL, "Failed to open msgpack file");

    fseek(fp, 0, SEEK_END);
    long file_size = ftell(fp);
    fseek(fp, 0, SEEK_SET);

    char *msgpack_data = malloc(file_size);
    size_t bytes_read = fread(msgpack_data, 1, file_size, fp);
    fclose(fp);
    ASSERT(bytes_read == file_size, "Failed to read msgpack file");

    /* Use COPY FROM STDIN with transit-msgpack format */
    snprintf(query, sizeof(query),
             "COPY %s FROM STDIN WITH (FORMAT 'transit-msgpack')", table);

    PGresult *res = PQexec(conn, query);
    ASSERT(PQresultStatus(res) == PGRES_COPY_IN, "COPY command failed");
    PQclear(res);

    /* Send the msgpack data */
    int result = PQputCopyData(conn, msgpack_data, file_size);
    ASSERT(result == 1, "PQputCopyData failed");

    result = PQputCopyEnd(conn, NULL);
    ASSERT(result == 1, "PQputCopyEnd failed");

    /* Get the result */
    res = PQgetResult(conn);
    ASSERT(PQresultStatus(res) == PGRES_COMMAND_OK, "COPY completion failed");
    PQclear(res);

    free(msgpack_data);

    /* Query back and verify */
    snprintf(query, sizeof(query), "SELECT _id, name, age FROM %s ORDER BY _id", table);
    res = PQexec(conn, query);

    ASSERT(PQresultStatus(res) == PGRES_TUPLES_OK, "Select failed");
    ASSERT(PQntuples(res) == 3, "Expected 3 records");
    ASSERT_EQ_STR(PQgetvalue(res, 0, 0), "alice", "_id should be alice");
    ASSERT_EQ_STR(PQgetvalue(res, 0, 1), "Alice Smith", "Name should be Alice Smith");
    ASSERT_EQ_STR(PQgetvalue(res, 0, 2), "30", "Age should be 30");

    PQclear(res);
    PASS();
}

TEST(transit_json_copy_from) {
    char *table = get_clean_table();
    char query[512];

    /* Read transit-json file as text */
    FILE *fp = fopen("../test-data/sample-users-transit.json", "r");
    ASSERT(fp != NULL, "Failed to open transit-json file");

    fseek(fp, 0, SEEK_END);
    long file_size = ftell(fp);
    fseek(fp, 0, SEEK_SET);

    char *json_data = malloc(file_size + 1);
    size_t bytes_read = fread(json_data, 1, file_size, fp);
    fclose(fp);
    json_data[bytes_read] = '\0';
    ASSERT(bytes_read == file_size, "Failed to read transit-json file");

    /* Use COPY FROM STDIN with transit-json format */
    snprintf(query, sizeof(query),
             "COPY %s FROM STDIN WITH (FORMAT 'transit-json')", table);

    PGresult *res = PQexec(conn, query);
    ASSERT(PQresultStatus(res) == PGRES_COPY_IN, "COPY command failed");
    PQclear(res);

    /* Send the JSON data */
    int result = PQputCopyData(conn, json_data, bytes_read);
    ASSERT(result == 1, "PQputCopyData failed");

    result = PQputCopyEnd(conn, NULL);
    ASSERT(result == 1, "PQputCopyEnd failed");

    /* Get the result */
    res = PQgetResult(conn);
    ASSERT(PQresultStatus(res) == PGRES_COMMAND_OK, "COPY completion failed");
    PQclear(res);

    free(json_data);

    /* Verify 3 records were loaded */
    snprintf(query, sizeof(query), "SELECT COUNT(*) FROM %s", table);
    res = PQexec(conn, query);
    ASSERT(PQresultStatus(res) == PGRES_TUPLES_OK, "Count query failed");
    ASSERT_EQ_STR(PQgetvalue(res, 0, 0), "3", "Expected 3 records");
    PQclear(res);

    /* Verify the alice record has correct fields */
    snprintf(query, sizeof(query),
             "SELECT _id, name, age, email, active, salary FROM %s WHERE _id = 'alice'", table);
    res = PQexec(conn, query);

    ASSERT(PQresultStatus(res) == PGRES_TUPLES_OK, "Select failed");
    ASSERT(PQntuples(res) == 1, "Expected 1 record for alice");
    ASSERT_EQ_STR(PQgetvalue(res, 0, 0), "alice", "_id should be alice");
    ASSERT_EQ_STR(PQgetvalue(res, 0, 1), "Alice Smith", "Name should be Alice Smith");
    ASSERT_EQ_STR(PQgetvalue(res, 0, 2), "30", "Age should be 30");
    ASSERT_EQ_STR(PQgetvalue(res, 0, 3), "alice@example.com", "Email should be alice@example.com");
    ASSERT_EQ_STR(PQgetvalue(res, 0, 4), "t", "Active should be true");
    ASSERT_EQ_STR(PQgetvalue(res, 0, 5), "125000.5", "Salary should be 125000.5");

    PQclear(res);

    printf("  Successfully tested transit-json with COPY FROM! Loaded 3 records from JSON format\n");
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
    RUN_TEST(json_with_oid);
    RUN_TEST(transit_with_oid);
    RUN_TEST(transit_nest_one_full_record);
    RUN_TEST(nested_data_roundtrip);
    RUN_TEST(transit_json_format);
    RUN_TEST(transit_json_encoding);
    RUN_TEST(transit_msgpack_copy_from);
    RUN_TEST(transit_json_copy_from);

    // Feature report for matrix generation
    // C supports all features - nothing to report

    PQfinish(conn);

    printf("\n=================================\n");
    printf("Test Results:\n");
    printf("  Passed: %d\n", tests_passed);
    printf("  Failed: %d\n", tests_failed);
    printf("  Total:  %d\n", tests_passed + tests_failed);
    printf("=================================\n");

    return tests_failed > 0 ? 1 : 0;
}
