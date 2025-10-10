defmodule XTDBTest do
  use ExUnit.Case

  @db_config [
    hostname: "xtdb",
    port: 5432,
    database: "xtdb",
    username: "xtdb"
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

    # Insert each user
    for user <- users do
      Postgrex.query!(
        pid,
        "INSERT INTO #{table} RECORDS {_id: '#{user["_id"]}', name: '#{user["name"]}', age: #{user["age"]}, active: #{user["active"]}}",
        []
      )
    end

    # Query back and verify
    result = Postgrex.query!(pid, "SELECT _id, name, age, active FROM #{table} ORDER BY _id", [])
    assert %Postgrex.Result{rows: rows} = result
    assert length(rows) == 3
    assert [["alice", "Alice Smith", 30, true] | _] = rows

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
    {:ok, pid} = Postgrex.start_link(@db_config)
    table = get_clean_table()

    # Load sample-users-transit.json
    {:ok, content} = File.read("../test-data/sample-users-transit.json")

    lines =
      content
      |> String.split("\n")
      |> Enum.reject(&(&1 == ""))

    # Parse and insert each line
    for line <- lines do
      {:ok, user_data} = Jason.decode(line)

      # Extract data from transit format
      # Transit format: ["^ ", "~:_id", "alice", "~:name", "Alice Smith", ...]
      ["^ " | pairs] = user_data
      map_data = Enum.chunk_every(pairs, 2) |> Map.new(fn [k, v] -> {k, v} end)

      id = map_data["~:_id"]
      name = map_data["~:name"]
      age = map_data["~:age"]
      active = map_data["~:active"]

      Postgrex.query!(
        pid,
        "INSERT INTO #{table} RECORDS {_id: '#{id}', name: '#{name}', age: #{age}, active: #{active}}",
        []
      )
    end

    # Query back and verify
    result = Postgrex.query!(pid, "SELECT _id, name, age, active FROM #{table} ORDER BY _id", [])
    assert %Postgrex.Result{rows: rows} = result
    assert length(rows) == 3
    assert [["alice", "Alice Smith", 30, true] | _] = rows

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
