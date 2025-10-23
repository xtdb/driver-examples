require 'sequel'
require 'json'

# Minimal Transit-JSON encoder for Ruby
class MinimalTransitEncoder
  def self.encode_value(value)
    case value
    when Hash
      encode_map(value)
    when Array
      encoded = value.map { |v| encode_value(v) }
      "[#{encoded.join(',')}]"
    when String
      value.to_json
    when TrueClass
      'true'
    when FalseClass
      'false'
    when Numeric
      value.to_s
    when Time, Date, DateTime
      "\"~t#{value.iso8601}\""
    when NilClass
      'null'
    else
      value.to_s.to_json
    end
  end

  def self.encode_map(data)
    pairs = []
    data.each do |key, value|
      pairs << "\"~:#{key}\""
      pairs << encode_value(value)
    end
    "[\"^ \",#{pairs.join(',')}]"
  end
end

# Transit decoder for parsing nested structures
class TransitDecoder
  def self.decode(value)
    return value unless value.is_a?(String)

    # Try to parse as JSON first (handles both transit-JSON and regular JSON)
    begin
      data = JSON.parse(value)
      return decode_value(data)
    rescue JSON::ParserError
      # Not JSON, continue to other formats
    end

    # Check for PostgreSQL array format: {val1,val2,...}
    if value.start_with?('{') && value.end_with?('}')
      return parse_pg_array(value)
    end

    # Return as-is if no format matches
    value
  end

  def self.parse_pg_array(str)
    # Remove braces
    content = str[1...-1]
    return [] if content.empty?

    # Split by comma and strip quotes from each element
    content.split(',').map { |v| v.strip.gsub(/^"|"$/, '') }
  end

  def self.decode_value(data)
    case data
    when Array
      if data.length == 2 && data[0].is_a?(String) && data[0].start_with?('~#')
        # Transit tagged value: ["~#tag", value]
        # For dates like ["~#time/zoned-date-time", "2020-01-15T00:00Z[UTC]"]
        # Extract just the value
        decode_value(data[1])
      elsif data[0] == "^ "
        # Transit map
        decode_map(data)
      else
        # Regular array
        data.map { |v| decode_value(v) }
      end
    when String
      if data.start_with?('~:')
        # Keyword - remove prefix
        data[2..-1]
      elsif data.start_with?('~t')
        # Date - remove prefix
        data[2..-1]
      else
        data
      end
    else
      data
    end
  end

  def self.decode_map(data)
    result = {}
    i = 1
    while i < data.length
      key = decode_value(data[i])
      value = decode_value(data[i + 1])
      result[key] = value
      i += 2
    end
    result
  end
end

RSpec.describe "Transit-JSON Operations" do
  let(:db) { Sequel.connect("xtdb://xtdb:5432/xtdb?fallback_output_format=transit") }
  let(:table) { "test_table_#{Time.now.to_i}_#{rand(10000)}" }

  after(:each) do
    db.disconnect if db
  end

  it "understands transit-JSON format" do
    data = {'_id' => 'transit1', 'name' => 'Transit User', 'age' => 42, 'active' => true}
    transit_json = MinimalTransitEncoder.encode_map(data)

    # Verify it creates proper transit format
    expect(transit_json).to include('["^ "')
    expect(transit_json).to include('"~:_id"')
    expect(transit_json).to include('"~:name"')

    # Insert using RECORDS curly brace syntax
    db << "INSERT INTO #{table} RECORDS {_id: 'transit1', name: 'Transit User', age: 42, active: true}"

    rows = db["SELECT _id, name, age, active FROM #{table} WHERE _id = 'transit1'"].all

    expect(rows[0][:_id]).to eq('transit1')
    expect(rows[0][:name]).to eq('Transit User')
    expect(rows[0][:age]).to eq(42)
    expect(rows[0][:active]).to be true
  end

  # Use pg gem's hash format for parameters: {value:, type:, format:}
  # where type is the OID. This bypasses Sequel and uses pg directly.
  it "parses sample-users-transit.json file" do
    # Load transit-JSON file
    transit_path = File.join(File.dirname(__FILE__), '../../test-data/sample-users-transit.json')
    lines = File.readlines(transit_path)

    # Get raw pg connection from Sequel
    conn = db.synchronize { |c| c }

    lines.each do |line|
      line = line.strip
      next if line.empty?

      # Insert using transit OID (16384) with single parameter
      # Use pg gem's hash format: {value:, type: OID, format: 0 for text}
      conn.exec_params("INSERT INTO #{table} RECORDS $1",
                       [{value: line, type: 16384, format: 0}])
    end

    # Query back and verify - use Sequel for querying
    rows = db["SELECT _id, name, age, active, email, salary, tags, metadata FROM #{table} ORDER BY _id"].all

    expect(rows.length).to eq(3)

    # Verify first record (alice) with all fields
    alice = rows[0]
    expect(alice[:_id]).to eq('alice')
    expect(alice[:name]).to eq('Alice Smith')
    expect(alice[:age]).to eq(30)
    expect(alice[:active]).to be true
    expect(alice[:email]).to eq('alice@example.com')
    expect(alice[:salary]).to be_within(0.01).of(125000.5)

    # Verify nested array (tags) - With transit output format, properly typed
    tags = TransitDecoder.decode(alice[:tags])
    expect(tags).to be_a(Array)
    expect(tags).to include('admin')
    expect(tags).to include('developer')
    expect(tags.length).to eq(2)

    # Verify nested object (metadata) - With transit output format, properly typed
    metadata = TransitDecoder.decode(alice[:metadata])
    expect(metadata).to be_a(Hash)
    expect(metadata['department']).to eq('Engineering')
    expect(metadata['level']).to eq(5)
    expect(metadata['joined']).to eq('2020-01-15T00:00Z')
  end

  it "handles transit encoding for various types" do
    data = {
      '_id' => 'types_test',
      'string' => 'hello',
      'number' => 42,
      'bool' => true,
      'array' => [1, 2, 3]
    }

    transit_json = MinimalTransitEncoder.encode_map(data)

    # Verify encoding
    expect(transit_json).to include('"hello"')
    expect(transit_json).to include('42')
    expect(transit_json).to include('true')
    expect(transit_json).to include('[1,2,3]')

    # Insert data
    db << "INSERT INTO #{table} RECORDS {_id: 'types_test', string_val: 'hello', number_val: 42, bool_val: true}"

    rows = db["SELECT _id FROM #{table} WHERE _id = 'types_test'"].all

    expect(rows.length).to eq(1)
  end

  it "parses sample-users-transit.msgpack file" do
    # Load transit-msgpack file (binary)
    msgpack_path = File.join(File.dirname(__FILE__), '../../test-data/sample-users-transit.msgpack')
    msgpack_data = File.binread(msgpack_path)

    # Get raw pg connection from Sequel
    conn = db.synchronize { |c| c }

    # Use COPY FROM STDIN with transit-msgpack format (without explicit transaction)
    conn.exec("COPY #{table} FROM STDIN WITH (FORMAT 'transit-msgpack')")
    conn.put_copy_data(msgpack_data)
    result = conn.put_copy_end

    # Check if copy succeeded
    if result.is_a?(String) && !result.empty?
      raise "COPY failed: #{result}"
    end

    # Get the result to ensure command completed
    while res = conn.get_result
      if res.result_status != PG::PGRES_COMMAND_OK
        raise "COPY failed with status: #{res.result_status}"
      end
    end

    # Query back and verify - use Sequel for querying
    rows = db["SELECT _id, name, age FROM #{table} ORDER BY _id"].all

    expect(rows.length).to eq(3)
    expect(rows[0][:_id]).to eq('alice')
    expect(rows[0][:name]).to eq('Alice Smith')
    expect(rows[0][:age]).to eq(30)
  end

  it "parses sample-users-transit.json file via COPY FROM" do
    # Load transit-JSON file
    json_path = File.join(File.dirname(__FILE__), '../../test-data/sample-users-transit.json')
    json_data = File.read(json_path)

    # Get raw pg connection from Sequel
    conn = db.synchronize { |c| c }

    # Use COPY FROM STDIN with transit-json format
    conn.exec("COPY #{table} FROM STDIN WITH (FORMAT 'transit-json')")
    conn.put_copy_data(json_data)
    result = conn.put_copy_end

    # Check if copy succeeded
    if result.is_a?(String) && !result.empty?
      raise "COPY failed: #{result}"
    end

    # Get the result to ensure command completed
    while res = conn.get_result
      if res.result_status != PG::PGRES_COMMAND_OK
        raise "COPY failed with status: #{res.result_status}"
      end
    end

    # Query back and verify - use Sequel for querying
    rows = db["SELECT _id, name, age, active, email, salary, tags, metadata FROM #{table} ORDER BY _id"].all

    # Verify 3 records are loaded
    expect(rows.length).to eq(3)

    # Verify the alice record has correct fields
    alice = rows[0]
    expect(alice[:_id]).to eq('alice')
    expect(alice[:name]).to eq('Alice Smith')
    expect(alice[:age]).to eq(30)
    expect(alice[:active]).to be true
    expect(alice[:email]).to eq('alice@example.com')
    expect(alice[:salary]).to be_within(0.01).of(125000.5)

    # Verify nested array (tags)
    tags = TransitDecoder.decode(alice[:tags])
    expect(tags).to be_a(Array)
    expect(tags).to include('admin')
    expect(tags).to include('developer')
    expect(tags.length).to eq(2)

    # Verify nested object (metadata)
    metadata = TransitDecoder.decode(alice[:metadata])
    expect(metadata).to be_a(Hash)
    expect(metadata['department']).to eq('Engineering')
    expect(metadata['level']).to eq(5)
    expect(metadata['joined']).to match(/2020-01-15/)  # Accept both date-only and datetime formats

    # Print success message
    puts "\n✅ Successfully loaded 3 records via COPY FROM with FORMAT 'transit-json'"
    puts "   Alice record verified with all fields including nested structures"
  end

  it "uses NEST_ONE to decode entire record with transit fallback" do
    # Load transit-JSON file
    transit_path = File.join(File.dirname(__FILE__), '../../test-data/sample-users-transit.json')
    lines = File.readlines(transit_path)

    # Get raw pg connection from Sequel
    conn = db.synchronize { |c| c }

    lines.each do |line|
      line = line.strip
      next if line.empty?

      # Insert using transit OID (16384)
      conn.exec_params("INSERT INTO #{table} RECORDS $1",
                       [{value: line, type: 16384, format: 0}])
    end

    # Query using NEST_ONE to get entire record as a single nested object
    result = db["SELECT NEST_ONE(FROM #{table} WHERE _id = 'alice') AS r"].first

    expect(result).not_to be_nil

    # The entire record comes back as a transit-JSON string that needs to be decoded
    record_raw = result[:r]
    puts "\n✅ NEST_ONE returned entire record: #{record_raw.class.name}"
    puts "   Raw record: #{record_raw}"

    # Decode the transit-JSON string
    record = TransitDecoder.decode(record_raw)
    puts "   Decoded record: #{record.class.name}"

    # With transit fallback, the entire record should be properly typed
    expect(record).to be_a(Hash)

    # Verify all fields are accessible as native types
    expect(record['_id']).to eq('alice')
    expect(record['name']).to eq('Alice Smith')
    expect(record['age']).to eq(30)
    expect(record['active']).to be true
    expect(record['email']).to eq('alice@example.com')
    expect(record['salary']).to be_within(0.01).of(125000.5)

    # Nested array should be native Array
    expect(record['tags']).to be_a(Array)
    expect(record['tags']).to include('admin')
    expect(record['tags']).to include('developer')
    puts "   ✅ Nested array (tags) properly typed: #{record['tags']}"

    # Nested object should be native Hash
    expect(record['metadata']).to be_a(Hash)
    expect(record['metadata']['department']).to eq('Engineering')
    expect(record['metadata']['level']).to eq(5)

    # Verify joined date - after transit decoding, tagged values like ["~#time/zoned-date-time", "..."]
    # are decoded to just the value string
    joined_raw = record['metadata']['joined']
    puts "   Joined raw value: #{joined_raw} (type: #{joined_raw.class})"

    expect(joined_raw).to be_a(String)

    # The transit decoder extracts the value from ["~#time/zoned-date-time", "2020-01-15T00:00Z[UTC]"]
    # leaving us with just "2020-01-15T00:00Z[UTC]"
    # Parse the ISO datetime string
    require 'date'
    begin
      # Handle both datetime with timezone and plain dates
      parsed_date = if joined_raw.include?('T')
        DateTime.iso8601(joined_raw.split('[')[0]) # Remove [UTC] suffix if present
      else
        Date.iso8601(joined_raw)
      end
      puts "   ✅ Decoded joined date to #{parsed_date.class}: #{parsed_date}"

      # Verify it's the expected date
      expect(parsed_date.year).to eq(2020)
      expect(parsed_date.month).to eq(1)
      expect(parsed_date.day).to eq(15)
      puts "   ✅ Transit tagged date successfully decoded and verified"
    rescue ArgumentError => e
      fail "Failed to parse date #{joined_raw}: #{e}"
    end

    puts "   ✅ Nested object (metadata) properly typed: #{record['metadata']}"

    puts "\n✅ NEST_ONE with transit fallback successfully decoded entire record!"
    puts "   All fields accessible as native Ruby types"
  end

  it "zzz feature report" do
    # Report unsupported features for matrix generation. Runs last due to zzz prefix.
    # Ruby supports all features - nothing to report
  end
end
