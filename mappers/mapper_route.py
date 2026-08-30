import sys

for line in sys.stdin:
    parts = line.strip().split(",")
    if len(parts) != 10:
        continue
    try:
        pu = parts[4].strip()
        do = parts[5].strip()
        distance = float(parts[3])
        fare = float(parts[7])
        total = float(parts[9])
    except (ValueError, IndexError):
        continue

    if not pu or not do:
        continue
    if distance <= 0 or fare < 0 or total < 0:
        continue

    print(f"{pu}->{do}\t1\t{fare}\t0\t{total}\t{distance}")