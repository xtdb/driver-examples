require 'sequel'

DB = Sequel.connect("xtdb://xtdb:5432/xtdb")

DB << "INSERT INTO ruby_users RECORDS {_id: 'alice', name: 'Alice'}, {_id: 'bob', name: 'Bob'}"

puts "Users:"
DB["SELECT _id, name FROM ruby_users"].each do |row|
  puts "  * #{row[:_id]}: #{row[:name]}"
end

puts "\n✓ XTDB connection successful"
