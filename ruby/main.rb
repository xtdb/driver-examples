require 'sequel'
require 'sequel/extensions/pg_json'

def shift_days(n, from: Time.now)
  from + (60 * 60 * 24 * n) # Shift time by `n` days
end

# Connect to the XTDB-compatible PostgreSQL database
DB = Sequel.connect("xtdb://xtdb:5432/xtdb")

# Regular SQL
DB << "insert into users(_id, name) values(1, 'Jeremy')"
puts "All users after first insert:"
puts DB["select * from users"].all

# With placeholders
DB << Sequel.lit(<<-SQL, shift_days(14))
insert into users(_id, name, _valid_from) values(2, 'James', timestamp ?)
SQL

# Using Sequel DSL with the adapter's `as_of`
puts "Users as of 15 days from now:"
puts DB.as_of(valid: shift_days(15)).with_sql("select * from users").all

yesterday = DB[:users].as_of(valid: shift_days(-1))

# Insert using DSL
yesterday.insert(_id: 3, name: "Gert")
puts "All users:"
puts DB[:users].all
