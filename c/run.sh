make
# ...or if you want a one-liner instead of a Makefile:
# gcc -o trades trades.c $(pkg-config --cflags --libs libpq)
./trades -h xtdb -d xtdb -p 5432
