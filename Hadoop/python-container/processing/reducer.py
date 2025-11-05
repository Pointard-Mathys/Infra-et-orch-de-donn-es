import sys

current_type = None
current_count = 0

for line in sys.stdin:
    line = line.strip()
    weapon_type, count = line.split('\t')
    count = int(count)
    
    if current_type == weapon_type:
        current_count += count
    else:
        if current_type:
            print(f"{current_type}\t{current_count}")
        current_type = weapon_type
        current_count = count

# Dernière clé
if current_type:
    print(f"{current_type}\t{current_count}")