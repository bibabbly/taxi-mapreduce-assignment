import sys

for line in sys.stdin:
    parts = line.strip().split(",")
    if len(parts) != 10:
        continue
    try:
        payment_type = parts[6].strip()
        fare = float(parts[7])
        tip = float(parts[8])
        total = float(parts[9])
    except (ValueError, IndexError):
        continue

    if fare < 0 or total < 0 or tip < 0:
        continue
    if payment_type not in ("1", "2", "3", "4", "5", "6"):
        continue

    print(f"{payment_type}\t1\t{fare}\t{tip}\t{total}")