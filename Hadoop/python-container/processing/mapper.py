import sys
import csv

# Lire depuis stdin
reader = csv.DictReader(sys.stdin)

for row in reader:
    weapon_type = row['type']
    print(f"{weapon_type}\t1")  # émettre une paire type → 1