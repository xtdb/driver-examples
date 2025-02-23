gcc -o trades trades.c $(pkg-config --cflags --libs libpq)
./trades
