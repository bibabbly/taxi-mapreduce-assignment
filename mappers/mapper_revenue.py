import sys

for line in sys.stdin:
    parts = line.strip().split(",")
    if len(parts) != 10:
        continue
    try:
        pu_location = parts[4]
        distance = float(parts[3])
        fare = float(parts[7])
        tip = float(parts[8])
        total = float(parts[9])
    except (ValueError, IndexError):
        continue

    if fare < 0 or total < 0 or distance < 0:
        continue

    print(f"{pu_location}\t1\t{fare}\t{tip}\t{total}\t{distance}")