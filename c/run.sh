gcc -o trades trades.c $(pkg-config --cflags --libs libpq)
./trades -h xtdb -d xtdb -p 5432
