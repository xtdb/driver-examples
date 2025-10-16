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
    username: "xtdb",
    queue_target: 10000, # mitigation for https://github.com/xtdb/xtdb/issues/4878
    queue_interval: 10000
  ]

  # Transit config for transit-specific tests only
  @db_config_transit [
    hostname: "xtdb",
    port: 5432,
    database: "xtdb",
    username: "xtdb",
    parameters: [fallback_output_format: "transit"],
    types: XTDBTest.TransitTypes,
    queue_target: 10000, # mitigation for https://github.com/xtdb/xtdb/issues/4878
    queue_interval: 10000
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

  test "transit nest one full record" do
    {:ok, pid} = Postgrex.start_link(@db_config_transit)
    table = get_clean_table()

    # Load sample-users-transit.json
    {:ok, content} = File.read("../test-data/sample-users-transit.json")

    lines =
      content
      |> String.split("\n")
      |> Enum.reject(&(&1 == ""))

    # Insert using transit OID (16384) with single parameter per record
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

    # Query using NEST_ONE to get entire record as a single nested object
    result = Postgrex.query!(pid, "SELECT NEST_ONE(FROM #{table} WHERE _id = 'alice') AS r", [])

    assert %Postgrex.Result{rows: [[record_raw]]} = result

    IO.puts("\n✅ NEST_ONE returned entire record (raw): #{inspect(record_raw)}")

    # The entire record comes back as a transit-JSON string or map depending on the driver
    record = if is_binary(record_raw) do
      # Decode transit-JSON string
      {:ok, decoded} = Jason.decode(record_raw)
      decode_transit(decoded)
    else
      record_raw
    end

    # The entire record should be a native Map after decoding
    assert is_map(record), "Record should be a Map after decoding"

    IO.puts("   Decoded record keys: #{inspect(Map.keys(record))}")

    # With transit fallback, the entire record should be properly typed
    # Verify all fields are accessible as native types
    assert record["_id"] == "alice"
    assert record["name"] == "Alice Smith"
    assert record["age"] == 30
    assert record["active"] == true
    assert record["email"] == "alice@example.com"
    assert record["salary"] == 125000.5

    # Nested array should be native List
    assert is_list(record["tags"]), "Tags should be a native list"
    assert length(record["tags"]) == 2
    assert "admin" in record["tags"]
    assert "developer" in record["tags"]
    IO.puts("   ✅ Nested array (tags) properly typed: #{inspect(record["tags"])}")

    # Nested object should be native Map
    assert is_map(record["metadata"]), "Metadata should be a native map"
    metadata = record["metadata"]

    assert metadata["department"] == "Engineering"
    assert metadata["level"] == 5

    # Verify joined date - after transit decoding, tagged values like ["~#time/zoned-date-time", "..."]
    # are decoded to just the value string
    joined_raw = metadata["joined"]
    IO.puts("   Joined raw value: #{inspect(joined_raw)} (type: #{inspect(__MODULE__.type_of(joined_raw))})")

    assert is_binary(joined_raw), "Expected joined to be a string"

    # The transit decoder extracts the value from ["~#time/zoned-date-time", "2020-01-15T00:00Z[UTC]"]
    # leaving us with just "2020-01-15T00:00Z[UTC]"
    # Remove the [UTC] timezone annotation and normalize format
    date_str =
      joined_raw
      |> String.split("[")
      |> List.first()
      |> String.replace(~r/Z$/, "+00:00")  # Replace trailing Z with +00:00
      |> then(fn str ->
        # Add seconds if missing (HH:MM format -> HH:MM:SS format)
        if Regex.match?(~r/T\d{2}:\d{2}[\+\-]/, str) do
          String.replace(str, ~r/(T\d{2}:\d{2})([\+\-])/, "\\1:00\\2")
        else
          str
        end
      end)

    # Parse to Date, NaiveDateTime, or DateTime
    case DateTime.from_iso8601(date_str) do
      {:ok, parsed_date, _offset} ->
        IO.puts("   ✅ Decoded joined date to DateTime: #{inspect(parsed_date)}")

        # Verify it's the expected date
        assert parsed_date.year == 2020
        assert parsed_date.month == 1
        assert parsed_date.day == 15
        IO.puts("   ✅ Transit tagged date successfully decoded and verified")

      {:error, reason} ->
        # Try NaiveDateTime if DateTime parsing fails
        case NaiveDateTime.from_iso8601(date_str) do
          {:ok, parsed_date} ->
            IO.puts("   ✅ Decoded joined date to NaiveDateTime: #{inspect(parsed_date)}")
            assert parsed_date.year == 2020
            assert parsed_date.month == 1
            assert parsed_date.day == 15
            IO.puts("   ✅ Transit tagged date successfully decoded and verified")

          {:error, _} ->
            # Try Date if NaiveDateTime fails
            case Date.from_iso8601(date_str) do
              {:ok, parsed_date} ->
                IO.puts("   ✅ Decoded joined date to Date: #{inspect(parsed_date)}")
                assert parsed_date.year == 2020
                assert parsed_date.month == 1
                assert parsed_date.day == 15
                IO.puts("   ✅ Transit tagged date successfully decoded and verified")

              {:error, _} ->
                flunk("Failed to parse transit date #{date_str}: #{reason}")
            end
        end
    end

    IO.puts("   ✅ Nested object (metadata) properly typed: #{inspect(metadata)}")

    IO.puts("\n✅ NEST_ONE with transit fallback successfully decoded entire record!")
    IO.puts("   All fields accessible as native Elixir types")

    GenServer.stop(pid)
  end

  # Helper function to decode transit-JSON structures
  defp decode_transit(value) when is_list(value) do
    case value do
      # Transit map: ["^ ", key1, val1, key2, val2, ...]
      ["^ " | pairs] ->
        pairs
        |> Enum.chunk_every(2)
        |> Enum.map(fn
          [k, v] -> {decode_transit(k), decode_transit(v)}
          [k] -> {decode_transit(k), nil}
        end)
        |> Map.new()

      # Transit tagged value: ["~#tag", value]
      [tag, val] when is_binary(tag) ->
        if String.starts_with?(tag, "~#") do
          decode_transit(val)
        else
          # Regular two-element array
          [decode_transit(tag), decode_transit(val)]
        end

      # Regular array
      _ ->
        Enum.map(value, &decode_transit/1)
    end
  end

  defp decode_transit(value) when is_binary(value) do
    cond do
      # Transit keyword: "~:keyword"
      String.starts_with?(value, "~:") ->
        String.slice(value, 2..-1//1)

      # Transit date: "~tdate"
      String.starts_with?(value, "~t") ->
        String.slice(value, 2..-1//1)

      true ->
        value
    end
  end

  defp decode_transit(value), do: value

  # Helper function to get type name
  def type_of(value) do
    cond do
      is_binary(value) -> :string
      is_map(value) -> :map
      is_list(value) -> :list
      is_integer(value) -> :integer
      is_float(value) -> :float
      is_boolean(value) -> :boolean
      true -> :unknown
    end
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
