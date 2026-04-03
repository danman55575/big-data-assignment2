import sys

current_key = None
current_sum = 0

# Process sorted key-value pairs from mapper
for line in sys.stdin:
    parts = line.strip().split('\t')
    
    if len(parts) != 2:
        continue
        
    key, count_str = parts
    
    try:
        count = int(count_str)
    except ValueError:
        continue

    # Aggregate counts for the same key
    if current_key == key:
        current_sum += count
    else:
        # Emit previous key's total
        if current_key:
            print(f"{current_key}\t{current_sum}")
        current_key = key
        current_sum = count

# Emit the last key
if current_key:
    print(f"{current_key}\t{current_sum}")