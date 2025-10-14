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

    # Split by comma (simple parsing, doesn't handle nested arrays with commas)
    content.split(',').map(&:strip)
  end

  def self.decode_value(data)
    case data
    when Array
      if data[0] == "^ "
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
end
