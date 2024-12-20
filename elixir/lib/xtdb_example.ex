defmodule XTDBExample do
  def connect_and_query do
    {:ok, pid} =
      Postgrex.start_link(
        hostname: "xtdb",
        port: 5432,
        database: "xtdb"
      )

    # Insert query using XTDB's RECORDS syntax
    insert_query = """
    INSERT INTO users RECORDS {_id: 'jms', name: 'James'}, {_id: 'joe', name: 'Joe'}
    """

    select_query = "SELECT * FROM users"

    # Execute the insert query
    case Postgrex.query(pid, insert_query, []) do
      {:ok, _result} ->
        IO.puts("Insert successful")
      {:error, error} ->
        IO.puts("Error during insert: #{inspect(error)}")
    end

    # Execute the select query
    case Postgrex.query(pid, select_query, []) do
      {:ok, %Postgrex.Result{rows: rows}} ->
        IO.puts("Users:")
        Enum.each(rows, fn [id, name] -> IO.puts("  * #{id}: #{name}") end)

      {:error, error} ->
        IO.puts("Error during select: #{inspect(error)}")
    end
  end
end
