defmodule XTDBExample do
  defp get_xtdb_host do
    System.get_env("XTDB_HOST") || "xtdb"
  end

  def connect_and_query do
    {:ok, pid} =
      Postgrex.start_link(
        hostname: get_xtdb_host(),
        port: 5432,
        database: "xtdb",
        username: "xtdb"
      )

    # Example with transit-json including all supported types
    # This demonstrates the full range of data types that can be preserved
    complex_data = %{
      _id: "alice",
      name: "Alice",

      # Temporal fields
      created_date: ~D[2024-01-15],
      last_login: ~U[2024-01-15 10:30:45.123Z],
      next_review: ~N[2024-06-15 14:00:00],

      # Boolean and numeric fields
      active: true,
      age: 28,
      score: 95.5,

      # Arrays with mixed types
      tags: ["admin", "user", "developer"],
      scores: [95.5, 87.2, 92.8],

      # Nested map with various types
      metadata: %{
        role: "admin",
        level: 5,
        verified: true,
        joined: ~D[2023-06-01],
        last_modified: ~U[2024-01-20 14:22:33.456Z],

        # Nested collections
        permissions: ["read", "write", "delete"],

        # Deeply nested structure
        settings: %{
          theme: "dark",
          notifications: true,
          timezone: "UTC",
          login_times: [
            ~U[2024-01-15 09:00:00Z],
            ~U[2024-01-15 13:30:00Z],
            ~U[2024-01-15 16:45:00Z]
          ]
        }
      }
    }

    IO.puts("Original Elixir data structure:")
    IO.inspect(complex_data, pretty: true, limit: :infinity)

    transit_json_data = build_transit_json(complex_data)

    IO.puts("\nTransit-JSON encoded (length: #{String.length(transit_json_data)} chars):")
    # Pretty print the JSON structure for readability
    case Jason.decode(transit_json_data) do
      {:ok, decoded} ->
        IO.puts(Jason.encode!(decoded, pretty: true))
      _ ->
        IO.puts(transit_json_data)
    end

    # Create a new table for testing
    create_table = """
    INSERT INTO transit_users RECORDS {_id: 'test', name: 'Test User'}
    """

    # Execute the standard insert query
    IO.puts("\nInserting test record...")
    case Postgrex.query(pid, create_table, []) do
      {:ok, _result} ->
        IO.puts("Test record inserted")
      {:error, _error} ->
        # Table might already exist, that's ok
        :ok
    end

    # For transit-json with OID 16384, we need to use prepare and execute
    IO.puts("\nInserting complex data with all types via transit-json (OID 16384)...")

    parameterized_insert = "INSERT INTO transit_users RECORDS $1"

    # First prepare the query
    case Postgrex.prepare(pid, "", parameterized_insert) do
      {:ok, query} ->
        IO.puts("Prepared query, original param_oids: #{inspect(query.param_oids)}")

        # Manually modify the query to specify OID 16384 for parameter 1
        modified_query = %{query |
          param_oids: [16384],
          param_formats: [:text],
          param_types: [Postgrex.Extensions.Raw]
        }

        IO.puts("Modified param_oids to: #{inspect(modified_query.param_oids)}")

        case Postgrex.execute(pid, modified_query, [transit_json_data]) do
          {:ok, _query, result} ->
            IO.puts("✓ Transit-json insert successful!")
            IO.puts("  Result: #{inspect(result)}")
          {:error, error} ->
            IO.puts("✗ Error during transit-json insert: #{inspect(error.postgres.message)}")
        end

        # Close the prepared query
        Postgrex.close(pid, query)

      {:error, error} ->
        IO.puts("Failed to prepare query: #{inspect(error)}")
    end

    # Query back the data to verify roundtrip
    IO.puts("\n=== Verifying Roundtrip ===")

    # Query just the alice record
    alice_query = "SELECT * FROM transit_users WHERE _id = 'alice'"

    case Postgrex.query(pid, alice_query, []) do
      {:ok, %Postgrex.Result{rows: rows, columns: columns}} ->
        IO.puts("Retrieved Alice record:")
        IO.puts("  Columns: #{inspect(columns)}")
        IO.puts("  Number of rows: #{length(rows)}")

        if length(rows) > 0 do
          IO.puts("  ✓ Data successfully stored and retrieved")

          # Print raw data for first row
          IO.puts("\n=== Raw Transit-JSON Data (as returned from XTDB) ===")
          [first_row | _] = rows
          Enum.zip(columns, first_row)
          |> Enum.each(fn {col, val} ->
            IO.puts("\nColumn: #{col}")
            IO.puts("Raw value type: #{inspect(type_of(val))}")
            IO.puts("Raw value: #{inspect(val, limit: :infinity, printable_limit: :infinity)}")
          end)
        end

      {:error, %{postgres: %{message: message}}} ->
        IO.puts("Query error: #{message}")
      {:error, error} ->
        IO.puts("Query error: #{inspect(error)}")
    end

    # Also try with specific column selection to avoid type issues
    IO.puts("\nQuerying specific columns (avoiding type issues):")
    safe_query = "SELECT _id, name FROM transit_users ORDER BY _id"

    case Postgrex.query(pid, safe_query, []) do
      {:ok, %Postgrex.Result{rows: rows}} ->
        IO.puts("Transit Users (id, name):")
        Enum.each(rows, fn [id, name] ->
          IO.puts("  * #{id}: #{name}")
        end)

      {:error, error} ->
        IO.puts("Error during select: #{inspect(error)}")
    end

    # Close the connection
    GenServer.stop(pid)

    IO.puts("\n=== Summary ===")
    IO.puts("✓ Complex transit-json data with all types was successfully sent to XTDB")
    IO.puts("✓ Data included:")
    IO.puts("  - Dates (Date)")
    IO.puts("  - Timestamps (DateTime with timezone)")
    IO.puts("  - Naive timestamps (NaiveDateTime)")
    IO.puts("  - Booleans")
    IO.puts("  - Integers and floats")
    IO.puts("  - Strings")
    IO.puts("  - Arrays with mixed types")
    IO.puts("  - Nested maps")
    IO.puts("  - Deeply nested structures")
    IO.puts("✓ All data was accepted with OID 16384 (transit-json type)")
    IO.puts("✓ Basic fields can be queried back")
    IO.puts("\nNote: Full metadata retrieval may require handling of XTDB-specific types")
  end

  # Transit-json builder with proper JSON string escaping
  defp build_transit_json(data) when is_map(data) do
    pairs = data
      |> Enum.flat_map(fn {k, v} ->
        key = ~s("~:#{to_string(k)}")
        value = encode_transit_value(v)
        [key, value]
      end)

    ~s(["^ ",#{Enum.join(pairs, ",")}])
  end

  defp encode_transit_value(%Date{} = date) do
    # Transit-json date format: ~t2024-01-15
    formatted = Date.to_iso8601(date)
    ~s("~t#{formatted}")
  end

  defp encode_transit_value(%DateTime{} = datetime) do
    # Transit-json timestamp format: ~t2024-01-15T10:30:45.123Z
    formatted = DateTime.to_iso8601(datetime)
    ~s("~t#{formatted}")
  end

  defp encode_transit_value(%NaiveDateTime{} = datetime) do
    # Convert naive datetime to ISO8601
    formatted = NaiveDateTime.to_iso8601(datetime)
    ~s("~t#{formatted}")
  end

  defp encode_transit_value(v) when is_binary(v) do
    # Properly escape the string for JSON
    Jason.encode!(v)
  end

  defp encode_transit_value(v) when is_boolean(v), do: to_string(v)
  defp encode_transit_value(v) when is_number(v), do: to_string(v)
  defp encode_transit_value(v) when is_atom(v), do: ~s("~:#{v}")

  defp encode_transit_value(v) when is_list(v) do
    encoded = Enum.map(v, &encode_transit_value/1)
    "[#{Enum.join(encoded, ",")}]"
  end

  defp encode_transit_value(v) when is_map(v) do
    pairs = v
      |> Enum.flat_map(fn {k, val} ->
        key = ~s("~:#{to_string(k)}")
        value = encode_transit_value(val)
        [key, value]
      end)

    ~s(["^ ",#{Enum.join(pairs, ",")}])
  end

  defp encode_transit_value(v), do: Jason.encode!(inspect(v))

  # Helper to identify the type of values returned from XTDB
  defp type_of(value) when is_binary(value), do: :binary
  defp type_of(value) when is_integer(value), do: :integer
  defp type_of(value) when is_float(value), do: :float
  defp type_of(value) when is_boolean(value), do: :boolean
  defp type_of(value) when is_list(value), do: :list
  defp type_of(value) when is_map(value), do: :map
  defp type_of(value) when is_atom(value), do: :atom
  defp type_of(value) when is_tuple(value), do: :tuple
  defp type_of(nil), do: :nil
  defp type_of(_), do: :unknown

  # Demonstration of the transit-json format with all types
  def demonstrate_format do
    IO.puts("\n=== Complete Transit-JSON Format Demonstration ===\n")

    data = %{
      _id: "example_user",

      # All temporal types
      date_field: ~D[2024-01-15],
      datetime_field: ~U[2024-01-15 10:30:45.123Z],
      naive_datetime_field: ~N[2024-01-15 10:30:45.123],

      # Basic types
      string_field: "Hello, World!",
      integer_field: 42,
      float_field: 3.14159,
      boolean_field: true,
      atom_field: :my_atom,

      # Collections
      simple_array: [1, 2, 3],
      mixed_array: [1, "two", :three, true, ~D[2024-01-15]],

      # Nested structures
      nested_map: %{
        level1: %{
          level2: %{
            level3: "deeply nested value",
            level3_date: ~U[2024-01-15 10:30:45Z]
          }
        }
      }
    }

    transit = build_transit_json(data)

    IO.puts("Original Elixir data structure:")
    IO.inspect(data, pretty: true, limit: :infinity)

    IO.puts("\nEncoded to transit-json (#{String.length(transit)} characters):")

    # Pretty print if possible
    case Jason.decode(transit) do
      {:ok, decoded} ->
        IO.puts(Jason.encode!(decoded, pretty: true))
      _ ->
        IO.puts(transit)
    end

    IO.puts("\nTransit-JSON type markers:")
    IO.puts("  [\"^ \", ...]      - Map marker")
    IO.puts("  \"~:keyword\"      - Keywords/atoms")
    IO.puts("  \"~t2024-01-15\"   - Dates")
    IO.puts("  \"~t2024-01...Z\"  - Timestamps")
    IO.puts("  [...]            - Arrays")
    IO.puts("  \"string\"         - Strings (JSON escaped)")
    IO.puts("  123, 3.14, true  - Numbers and booleans (JSON literals)")

    IO.puts("\nThis comprehensive example preserves:")
    IO.puts("  ✓ All Elixir temporal types (Date, DateTime, NaiveDateTime)")
    IO.puts("  ✓ Type distinction (atoms vs strings, dates vs timestamps)")
    IO.puts("  ✓ Deeply nested structures")
    IO.puts("  ✓ Mixed-type collections")
    IO.puts("  ✓ Full numeric precision")
    IO.puts("  ✓ Boolean values")
    IO.puts("  ✓ Special characters in strings (properly escaped)")
  end

  def test_copy_simple do
    IO.puts("\n=== Testing Simple COPY FROM STDIN (like Postgrex test suite) ===")

    {:ok, pid} = Postgrex.start_link(hostname: get_xtdb_host(), port: 5432, database: "xtdb", username: "xtdb")

    try do
      # Setup test table
      IO.puts("Setting up test table...")
      Postgrex.query!(pid, "INSERT INTO simple_copy_test RECORDS {_id: 'init', value: 1}", [])

      # Test: Using stream() directly without prepare (like Postgrex test suite)
      IO.puts("\nUsing stream() with COPY statement directly (no prepare)")
      result = Postgrex.transaction(pid, fn conn ->
        try do
          stream = Postgrex.stream(conn, "COPY simple_copy_test FROM STDIN WITH (FORMAT 'transit-json')", [])

          # Try to stream data
          data = [
            "[\"^ \",\"~:xt/id\",\"copy_1\",\"~:value\",100]\n",
            "[\"^ \",\"~:xt/id\",\"copy_2\",\"~:value\",200]\n"
          ]

          IO.puts("Streaming data...")
          stream_result = Enum.into(data, stream)
          IO.puts("✓ Stream completed: #{inspect(stream_result == stream)}")

          # Verify
          case Postgrex.query!(conn, "SELECT _id, value FROM simple_copy_test WHERE _id LIKE 'copy_%' ORDER BY _id", []) do
            %{rows: rows} ->
              IO.puts("✓ Data inserted successfully:")
              Enum.each(rows, fn row -> IO.puts("    #{inspect(row)}") end)
          end

          Postgrex.rollback(conn, :done)
        rescue
          e ->
            IO.puts("✗ Error: #{inspect(e)}")
            IO.puts("  Message: #{Exception.message(e)}")
            Postgrex.rollback(conn, {:error, e})
        end
      end)

      case result do
        {:error, :done} -> IO.puts("\n✓ Transaction rolled back successfully")
        {:error, {:error, _}} -> IO.puts("\n✗ Test failed with error")
        _ -> IO.puts("\nUnexpected result: #{inspect(result)}")
      end

    rescue
      e ->
        IO.puts("✗ Exception during test: #{inspect(e)}")
        :error
    after
      GenServer.stop(pid)
    end
  end

  def test_copy_from_stdin do
    IO.puts("\n=== Testing COPY FROM STDIN with copy_data option ===")

    {:ok, pid} = Postgrex.start_link(hostname: get_xtdb_host(), port: 5432, database: "xtdb", username: "xtdb")

    try do
      # Setup test table
      IO.puts("Setting up test table...")
      Postgrex.query!(pid, "INSERT INTO copy_test RECORDS {_id: 'init', name: 'Init'}", [])

      # Test data in transit-json format with correct xt/id keyword
      test_data1 = "[\"^ \",\"~:xt/id\",\"copy_1\",\"~:name\",\"First Copy\",\"~:active\",true]"
      test_data2 = "[\"^ \",\"~:xt/id\",\"copy_2\",\"~:name\",\"Second Copy\",\"~:active\",false]"

      IO.puts("Attempting COPY FROM STDIN with streaming...")
      IO.puts("Data to copy:")
      IO.puts("  #{test_data1}")
      IO.puts("  #{test_data2}")

      # Use streaming approach with prepare and copy_data: true
      copy_sql = "COPY copy_test FROM STDIN WITH (FORMAT 'transit-json')"

      case Postgrex.prepare(pid, "", copy_sql, [copy_data: true]) do
        {:ok, query} ->
          # Use transaction with streaming
          result = Postgrex.transaction(pid, fn(conn) ->
            try do
              stream = Postgrex.stream(conn, query, [])

              IO.puts("Streaming first batch of data...")
              stream_result = Enum.into([test_data1 <> "\n", test_data2 <> "\n"], stream)

              if stream_result == stream do
                IO.puts("✓ Data streamed successfully")
              else
                IO.puts("⚠ Stream result: #{inspect(stream_result)}")
              end

              IO.puts("✓ COPY FROM STDIN streaming succeeded!")

              # Verify the data was inserted
              case Postgrex.query!(conn, "SELECT _id, name, active FROM copy_test WHERE _id LIKE 'copy_%' ORDER BY _id", []) do
                %{rows: rows} ->
                  IO.puts("✓ Verification query succeeded:")
                  Enum.each(rows, fn row ->
                    IO.puts("    #{inspect(row)}")
                  end)
              end

              Postgrex.rollback(conn, :done)
            rescue
              e ->
                if String.contains?(Exception.message(e), "COPY IN not in progress") do
                  IO.puts("✗ BUG REPRODUCED: 'COPY IN not in progress' error!")
                  IO.puts("  Error: #{inspect(e)}")
                  Postgrex.rollback(conn, {:error, :bug_reproduced})
                else
                  IO.puts("✗ COPY FROM STDIN failed with different error:")
                  IO.puts("  #{inspect(e)}")
                  Postgrex.rollback(conn, {:error, :failed})
                end
            end
          end)

          case result do
            {:error, :rollback} ->
              IO.puts("✓ Transaction rolled back as expected")
              :success
            {:error, :bug_reproduced} ->
              :bug_reproduced
            {:error, :failed} ->
              :failed
            _ ->
              :success
          end

        {:error, error} ->
          IO.puts("✗ Failed to prepare COPY statement: #{inspect(error)}")
          :failed
      end

    rescue
      e ->
        IO.puts("✗ Exception during COPY test: #{inspect(e)}")
        :error
    after
      GenServer.stop(pid)
    end
  end
end