import sys
from datetime import date

DAYS = ["1_Monday", "2_Tuesday", "3_Wednesday", "4_Thursday",
        "5_Friday", "6_Saturday", "7_Sunday"]

for line in sys.stdin:
    parts = line.strip().split(",")
    if len(parts) != 10:
        continue
    try:
        pickup = parts[0]
        year = int(pickup[0:4])
        month = int(pickup[5:7])
        day = int(pickup[8:10])
        total = float(parts[9])
    except (ValueError, IndexError):
        continue

    if year != 2024 or month not in (1, 2, 3):
        continue
    if total < 0:
        continue

    try:
        weekday = date(year, month, day).weekday()
    except ValueError:
        continue

    print(f"{DAYS[weekday]}\t1\t{total}")