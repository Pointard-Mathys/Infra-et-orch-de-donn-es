#!/usr/bin/env python3
import sys
import csv

# lire depuis stdin
reader = csv.DictReader(sys.stdin)
for row in reader:
    # on va compter les types d'armes
    print(f"{row['type']}\t1")
