import sys

for line in sys.stdin:
    parts = line.strip().split(",")
    if len(parts) != 10:
        continue
    try:
        pickup = parts[0]
        hour = int(pickup[11:13])
        if hour < 0 or hour > 23:
            continue
        print(f"{hour:02d}\t1")
    except (ValueError, IndexError):
        continue