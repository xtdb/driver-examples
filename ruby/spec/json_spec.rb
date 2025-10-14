require 'sequel'
require 'json'

RSpec.describe "JSON Operations" do
  # JSON tests use standard connection (no transit fallback needed for JSON OID 114)
  let(:db) { Sequel.connect("xtdb://xtdb:5432/xtdb") }
  let(:table) { "test_table_#{Time.now.to_i}_#{rand(10000)}" }

  after(:each) do
    db.disconnect if db
  end

  it "handles records with multiple fields" do
    db << "INSERT INTO #{table} RECORDS {_id: 'user1', name: 'Alice', age: 30, active: true}"

    rows = db["SELECT _id, name, age, active FROM #{table} WHERE _id = 'user1'"].all

    expect(rows.length).to eq(1)
    expect(rows[0][:_id]).to eq('user1')
    expect(rows[0][:name]).to eq('Alice')
    expect(rows[0][:age]).to eq(30)
    expect(rows[0][:active]).to be true
  end

  # Use pg gem's hash format for parameters: {value:, type: OID, format:}
  it "loads and roundtrips sample data" do
    # Load sample-users.json
    sample_path = File.join(File.dirname(__FILE__), '../../test-data/sample-users.json')
    users = JSON.parse(File.read(sample_path))

    # Get raw pg connection from Sequel
    conn = db.synchronize { |c| c }

    # Insert using JSON OID (114) with single parameter per record
    # Use pg gem's hash format: {value:, type: 114, format: 0}
    users.each do |user|
      conn.exec_params("INSERT INTO #{table} RECORDS $1",
                       [{value: user.to_json, type: 114, format: 0}])
    end

    # Query back and verify with ALL fields including nested data
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

    # Verify nested array (tags) - Should come back as native Array
    tags = alice[:tags]
    expect(tags).to be_a(Array)
    expect(tags).to include('admin')
    expect(tags).to include('developer')
    expect(tags.length).to eq(2)

    # Verify nested object (metadata) - Should come back as native Hash
    metadata = alice[:metadata]
    expect(metadata).to be_a(Hash)
    expect(metadata['department']).to eq('Engineering')
    expect(metadata['level']).to eq(5)
    # Date field should be a simple string in JSON
    expect(metadata['joined']).to eq('2020-01-15')
  end
end
