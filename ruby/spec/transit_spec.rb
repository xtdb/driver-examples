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

  def self.decode_transit_line(line)
    data = JSON.parse(line)
    return data unless data.is_a?(Array) && data[0] == "^ "

    result = {}
    i = 1
    while i < data.length
      key = data[i]
      value = data[i + 1]

      # Remove ~: prefix from keywords
      key = key[2..-1] if key.is_a?(String) && key.start_with?('~:')

      # Handle ~t dates
      value = value[2..-1] if value.is_a?(String) && value.start_with?('~t')

      result[key] = value
      i += 2
    end

    result
  end
end

RSpec.describe "Transit-JSON Operations" do
  let(:db) { Sequel.connect("xtdb://xtdb:5432/xtdb") }
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

  it "parses sample-users-transit.json file" do
    # Load and parse transit-JSON file
    transit_path = File.join(File.dirname(__FILE__), '../../test-data/sample-users-transit.json')
    lines = File.readlines(transit_path)

    lines.each do |line|
      line = line.strip
      next if line.empty?

      # Decode transit to Ruby hash
      user_data = MinimalTransitEncoder.decode_transit_line(line)

      # Insert using RECORDS curly brace format
      db << "INSERT INTO #{table} RECORDS {_id: '#{user_data['_id']}', name: '#{user_data['name']}', age: #{user_data['age']}, active: #{user_data['active']}}"
    end

    # Query back and verify
    rows = db["SELECT _id, name, age, active FROM #{table} ORDER BY _id"].all

    expect(rows.length).to eq(3)
    expect(rows[0][:_id]).to eq('alice')
    expect(rows[0][:name]).to eq('Alice Smith')
    expect(rows[0][:age]).to eq(30)
    expect(rows[0][:active]).to be true
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
