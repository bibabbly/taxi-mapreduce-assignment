import sys

current_hour = None
current_count = 0

for line in sys.stdin:
    try:
        hour, count = line.strip().split("\t")
        count = int(count)
    except ValueError:
        continue

    if hour == current_hour:
        current_count += count
    else:
        if current_hour is not None:
            print(f"{current_hour}\t{current_count}")
        current_hour = hour
        current_count = count

if current_hour is not None:
    print(f"{current_hour}\t{current_count}")