require 'sequel'
require 'json'

RSpec.describe "JSON Operations" do
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

  it "loads and roundtrips sample data" do
    # Load sample-users.json
    sample_path = File.join(File.dirname(__FILE__), '../../test-data/sample-users.json')
    users = JSON.parse(File.read(sample_path))

    # Insert each user using RECORDS syntax
    users.each do |user|
      db << "INSERT INTO #{table} RECORDS {_id: '#{user['_id']}', name: '#{user['name']}', age: #{user['age']}, active: #{user['active']}}"
    end

    # Query back and verify
    rows = db["SELECT _id, name, age, active FROM #{table} ORDER BY _id"].all

    expect(rows.length).to eq(3)
    expect(rows[0][:_id]).to eq('alice')
    expect(rows[0][:name]).to eq('Alice Smith')
    expect(rows[0][:age]).to eq(30)
    expect(rows[0][:active]).to be true
  end
end
