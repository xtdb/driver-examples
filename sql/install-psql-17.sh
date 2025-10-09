# Install PostgreSQL client (Arch Linux)
sudo pacman -S --noconfirm postgresql
psql -h xtdb xtdb -c "SELECT 1"
