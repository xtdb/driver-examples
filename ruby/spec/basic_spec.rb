require 'sequel'

RSpec.describe "XTDB Ruby Driver" do
  let(:xtdb_host) { ENV['XTDB_HOST'] || 'xtdb' }
  let(:db) { Sequel.connect("xtdb://#{xtdb_host}:5432/xtdb") }
  let(:table) { "test_table_#{Time.now.to_i}_#{rand(10000)}" }

  after(:each) do
    db.disconnect if db
  end

  it "connects to database" do
    result = db["SELECT 1 as test"].first
    expect(result[:test]).to eq(1)
  end

  it "inserts and queries records" do
    db << "INSERT INTO #{table} RECORDS {_id: 'test1', value: 'hello'}, {_id: 'test2', value: 'world'}"

    rows = db["SELECT _id, value FROM #{table} ORDER BY _id"].all

    expect(rows.length).to eq(2)
    expect(rows[0][:_id]).to eq("test1")
    expect(rows[0][:value]).to eq("hello")
  end

  it "filters with WHERE clause" do
    db << "INSERT INTO #{table} (_id, age) VALUES (1, 25), (2, 35), (3, 45)"

    rows = db["SELECT _id FROM #{table} WHERE age > 30 ORDER BY _id"].all

    expect(rows.length).to eq(2)
  end

  it "counts records" do
    db << "INSERT INTO #{table} RECORDS {_id: 1}, {_id: 2}, {_id: 3}"

    count = db["SELECT COUNT(*) as count FROM #{table}"].first[:count]

    expect(count).to eq(3)
  end

  it "handles parameterized queries" do
    db << "INSERT INTO #{table} RECORDS {_id: 'param1', name: 'Test User', age: 30}"

    rows = db["SELECT _id, name, age FROM #{table} WHERE _id = ?", 'param1'].all

    expect(rows.length).to eq(1)
    expect(rows[0][:name]).to eq("Test User")
    expect(rows[0][:age]).to eq(30)
  end
end
