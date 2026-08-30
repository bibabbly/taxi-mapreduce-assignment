import sys


def bucket(miles):
    if miles < 2:
        return "1_0-2mi"
    if miles < 5:
        return "2_2-5mi"
    if miles < 10:
        return "3_5-10mi"
    if miles < 20:
        return "4_10-20mi"
    return "5_20mi+"


for line in sys.stdin:
    parts = line.strip().split(",")
    if len(parts) != 10:
        continue
    try:
        distance = float(parts[3])
        fare = float(parts[7])
        tip = float(parts[8])
        total = float(parts[9])
    except (ValueError, IndexError):
        continue

    if distance <= 0 or fare < 0 or total < 0:
        continue
    if distance > 200:
        continue

    print(f"{bucket(distance)}\t1\t{fare}\t{tip}\t{total}\t{distance}")