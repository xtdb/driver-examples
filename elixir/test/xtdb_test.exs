defmodule XTDBTest do
  use ExUnit.Case

  # Custom types module that includes the transit extension (for transit tests only)
  Postgrex.Types.define(
    XTDBTest.TransitTypes,
    [TransitExtension],
    []
  )

  # Standard config for JSON and basic tests (no transit fallback)
  @db_config [
    hostname: "xtdb",
    port: 5432,
    database: "xtdb",
    username: "xtdb"
  ]

  # Transit config for transit-specific tests only
  @db_config_transit [
    hostname: "xtdb",
    port: 5432,
    database: "xtdb",
    username: "xtdb",
    parameters: [fallback_output_format: "transit"],
    types: XTDBTest.TransitTypes
  ]

  defp get_clean_table do
    "test_table_#{System.system_time(:millisecond)}_#{:rand.uniform(10000)}"
  end

  defp build_transit_json(data) when is_map(data) do
    pairs =
      data
      |> Enum.flat_map(fn {k, v} ->
        key = ~s("~:#{to_string(k)}")
        value = encode_transit_value(v)
        [key, value]
      end)

    ~s(["^ ",#{Enum.join(pairs, ",")}])
  end

  defp encode_transit_value(%Date{} = date) do
    formatted = Date.to_iso8601(date)
    ~s("~t#{formatted}")
  end

  defp encode_transit_value(%DateTime{} = datetime) do
    formatted = DateTime.to_iso8601(datetime)
    ~s("~t#{formatted}")
  end

  defp encode_transit_value(%NaiveDateTime{} = datetime) do
    formatted = NaiveDateTime.to_iso8601(datetime)
    ~s("~t#{formatted}")
  end

  defp encode_transit_value(v) when is_binary(v), do: Jason.encode!(v)
  defp encode_transit_value(v) when is_boolean(v), do: to_string(v)
  defp encode_transit_value(v) when is_number(v), do: to_string(v)
  defp encode_transit_value(v) when is_atom(v), do: ~s("~:#{v}")

  defp encode_transit_value(v) when is_list(v) do
    encoded = Enum.map(v, &encode_transit_value/1)
    "[#{Enum.join(encoded, ",")}]"
  end

  defp encode_transit_value(v) when is_map(v) do
    pairs =
      v
      |> Enum.flat_map(fn {k, val} ->
        key = ~s("~:#{to_string(k)}")
        value = encode_transit_value(val)
        [key, value]
      end)

    ~s(["^ ",#{Enum.join(pairs, ",")}])
  end

  defp encode_transit_value(v), do: Jason.encode!(inspect(v))

  # Basic Operations Tests

  test "connection" do
    {:ok, pid} = Postgrex.start_link(@db_config)

    result = Postgrex.query!(pid, "SELECT 1 as test", [])
    assert %Postgrex.Result{rows: [[1]]} = result

    GenServer.stop(pid)
  end

  test "insert and query" do
    {:ok, pid} = Postgrex.start_link(@db_config)
    table = get_clean_table()

    Postgrex.query!(
      pid,
      "INSERT INTO #{table} RECORDS {_id: 'test1', value: 'hello'}, {_id: 'test2', value: 'world'}",
      []
    )

    result = Postgrex.query!(pid, "SELECT _id, value FROM #{table} ORDER BY _id", [])
    assert %Postgrex.Result{rows: rows} = result
    assert length(rows) == 2
    assert [["test1", "hello"], ["test2", "world"]] = rows

    GenServer.stop(pid)
  end

  test "where clause" do
    {:ok, pid} = Postgrex.start_link(@db_config)
    table = get_clean_table()

    Postgrex.query!(pid, "INSERT INTO #{table} (_id, age) VALUES (1, 25), (2, 35), (3, 45)", [])

    result = Postgrex.query!(pid, "SELECT _id FROM #{table} WHERE age > 30 ORDER BY _id", [])
    assert %Postgrex.Result{rows: rows} = result
    assert length(rows) == 2

    GenServer.stop(pid)
  end

  test "count query" do
    {:ok, pid} = Postgrex.start_link(@db_config)
    table = get_clean_table()

    Postgrex.query!(pid, "INSERT INTO #{table} RECORDS {_id: 1}, {_id: 2}, {_id: 3}", [])

    result = Postgrex.query!(pid, "SELECT COUNT(*) as count FROM #{table}", [])
    assert %Postgrex.Result{rows: [[3]]} = result

    GenServer.stop(pid)
  end

  test "parameterized query" do
    {:ok, pid} = Postgrex.start_link(@db_config)
    table = get_clean_table()

    Postgrex.query!(
      pid,
      "INSERT INTO #{table} RECORDS {_id: 'param1', name: 'Test User', age: 30}",
      []
    )

    result =
      Postgrex.query!(pid, "SELECT _id, name, age FROM #{table} WHERE _id = $1", ["param1"])

    assert %Postgrex.Result{rows: [["param1", "Test User", 30]]} = result

    GenServer.stop(pid)
  end

  # JSON Tests

  test "json records" do
    {:ok, pid} = Postgrex.start_link(@db_config)
    table = get_clean_table()

    Postgrex.query!(
      pid,
      "INSERT INTO #{table} RECORDS {_id: 'user1', name: 'Alice', age: 30, active: true}",
      []
    )

    result = Postgrex.query!(pid, "SELECT _id, name, age, active FROM #{table} WHERE _id = 'user1'", [])
    assert %Postgrex.Result{rows: [["user1", "Alice", 30, true]]} = result

    GenServer.stop(pid)
  end

  test "load sample json" do
    {:ok, pid} = Postgrex.start_link(@db_config)
    table = get_clean_table()

    # Load sample-users.json
    {:ok, content} = File.read("../test-data/sample-users.json")
    {:ok, users} = Jason.decode(content)

    # Insert using JSON OID (114) with single parameter per record
    # Using the approach from xtdb_example.ex: prepare, modify param_oids, execute
    {:ok, query} = Postgrex.prepare(pid, "", "INSERT INTO #{table} RECORDS $1")

    # Manually modify the query to specify OID 114 for JSON
    modified_query = %{query |
      param_oids: [114],
      param_formats: [:text],
      param_types: [Postgrex.Extensions.Raw]
    }

    for user <- users do
      user_json = Jason.encode!(user)
      {:ok, _, _result} = Postgrex.execute(pid, modified_query, [user_json])
    end

    Postgrex.close(pid, query)

    # Query back and verify - get ALL columns including nested data
    result = Postgrex.query!(pid, "SELECT * FROM #{table} ORDER BY _id", [])
    assert %Postgrex.Result{rows: rows} = result
    assert length(rows) == 3

    # Verify first record (alice)
    [first_row | _] = rows
    columns = result.columns
    row_map = Enum.zip(columns, first_row) |> Map.new()

    assert row_map["_id"] == "alice"
    assert row_map["name"] == "Alice Smith"
    assert row_map["age"] == 30
    assert row_map["active"] == true
    assert row_map["email"] == "alice@example.com"
    assert row_map["salary"] == 125000.5

    # Verify nested array (tags) - With transit output format, properly typed
    assert is_list(row_map["tags"]), "Tags should be a list"
    assert length(row_map["tags"]) == 2
    assert "admin" in row_map["tags"]
    assert "developer" in row_map["tags"]
    IO.puts("✅ Tags properly typed as list: #{inspect(row_map["tags"])}")

    # Verify nested object (metadata) - With transit output format, properly typed
    assert is_map(row_map["metadata"]), "Metadata should be a map"
    metadata = row_map["metadata"]

    # Validate metadata fields
    assert metadata["department"] == "Engineering",
      "Expected department='Engineering', got #{inspect(metadata["department"])}"
    assert metadata["level"] == 5,
      "Expected level=5, got #{inspect(metadata["level"])}"
    assert is_binary(metadata["joined"]) or is_map(metadata["joined"]),
      "Expected joined field to be present"

    IO.puts("✅ Metadata properly typed as map: #{inspect(metadata)}")

    GenServer.stop(pid)
  end

  # Transit-JSON Tests

  test "transit json format" do
    {:ok, pid} = Postgrex.start_link(@db_config)
    table = get_clean_table()

    # Create transit-JSON
    data = %{_id: "transit1", name: "Transit User", age: 42, active: true}
    transit_json = build_transit_json(data)

    # Verify it contains transit markers
    assert String.contains?(transit_json, "~:")

    # Insert using RECORDS syntax
    Postgrex.query!(
      pid,
      "INSERT INTO #{table} RECORDS {_id: 'transit1', name: 'Transit User', age: 42, active: true}",
      []
    )

    result = Postgrex.query!(pid, "SELECT _id, name, age, active FROM #{table} WHERE _id = 'transit1'", [])
    assert %Postgrex.Result{rows: [["transit1", "Transit User", 42, true]]} = result

    GenServer.stop(pid)
  end

  test "parse transit json" do
    {:ok, pid} = Postgrex.start_link(@db_config_transit)
    table = get_clean_table()

    # Load sample-users-transit.json
    {:ok, content} = File.read("../test-data/sample-users-transit.json")

    lines =
      content
      |> String.split("\n")
      |> Enum.reject(&(&1 == ""))

    # Insert using transit OID (16384) with single parameter per record
    # Using the approach from xtdb_example.ex: prepare, modify param_oids, execute
    {:ok, query} = Postgrex.prepare(pid, "", "INSERT INTO #{table} RECORDS $1")

    # Manually modify the query to specify OID 16384 for transit-JSON
    modified_query = %{query |
      param_oids: [16384],
      param_formats: [:text],
      param_types: [Postgrex.Extensions.Raw]
    }

    for line <- lines do
      {:ok, _, _result} = Postgrex.execute(pid, modified_query, [line])
    end

    Postgrex.close(pid, query)

    # Query back and verify - get ALL columns including nested data
    result = Postgrex.query!(pid, "SELECT * FROM #{table} ORDER BY _id", [])
    assert %Postgrex.Result{rows: rows} = result
    assert length(rows) == 3

    # Verify first record (alice)
    [first_row | _] = rows
    columns = result.columns
    row_map = Enum.zip(columns, first_row) |> Map.new()

    assert row_map["_id"] == "alice"
    assert row_map["name"] == "Alice Smith"
    assert row_map["age"] == 30
    assert row_map["active"] == true
    assert row_map["email"] == "alice@example.com"
    assert row_map["salary"] == 125000.5

    # Verify nested array (tags) - With transit output format, properly typed
    assert is_list(row_map["tags"]), "Tags should be a list"
    assert length(row_map["tags"]) == 2
    assert "admin" in row_map["tags"]
    assert "developer" in row_map["tags"]
    IO.puts("✅ Tags properly typed as list: #{inspect(row_map["tags"])}")

    # Verify nested object (metadata) - With transit output format, properly typed
    assert is_map(row_map["metadata"]), "Metadata should be a map"
    metadata = row_map["metadata"]

    # Validate metadata fields
    assert metadata["department"] == "Engineering",
      "Expected department='Engineering', got #{inspect(metadata["department"])}"
    assert metadata["level"] == 5,
      "Expected level=5, got #{inspect(metadata["level"])}"

    # Validate joined date field - with transit output format, temporal types are properly decoded
    assert is_binary(metadata["joined"]),
      "Expected joined to be a string, got #{inspect(metadata["joined"])}"
    assert String.contains?(metadata["joined"], "2020-01-15"),
      "Expected joined to contain date 2020-01-15, got #{inspect(metadata["joined"])}"

    IO.puts("✅ Metadata properly typed as map with decoded temporal types: #{inspect(metadata)}")

    GenServer.stop(pid)
  end

  test "transit json encoding" do
    # Test transit encoding capabilities
    data = %{
      string: "hello",
      number: 42,
      bool: true,
      array: [1, 2, 3]
    }

    transit_json = build_transit_json(data)

    # Verify encoding
    assert String.contains?(transit_json, "hello")
    assert String.contains?(transit_json, "42")
    assert String.contains?(transit_json, "true")

    # Verify it can be parsed as JSON
    {:ok, _parsed} = Jason.decode(transit_json)
  end

  test "transit json with dates" do
    # Test date encoding
    data = %{
      _id: "date_test",
      date_field: ~D[2024-01-15],
      datetime_field: ~U[2024-01-15 10:30:45.123Z]
    }

    transit_json = build_transit_json(data)

    # Verify date formatting
    assert String.contains?(transit_json, "~t2024-01-15")
    assert String.contains?(transit_json, "~t2024-01-15T10:30:45")
  end
end
