import sys

current = None
trips = 0
revenue = 0.0


def emit(day, trips, revenue):
    label = day.split("_", 1)[1]
    avg = revenue / trips if trips else 0
    weekend = "Weekend" if label in ("Saturday", "Sunday") else "Weekday"
    print(f"{label}\t{trips}\t{revenue:.2f}\t{avg:.2f}\t{weekend}")


for line in sys.stdin:
    fields = line.strip().split("\t")
    if len(fields) != 3:
        continue
    try:
        day = fields[0]
        count = int(fields[1])
        total = float(fields[2])
    except ValueError:
        continue

    if day == current:
        trips += count
        revenue += total
    else:
        if current is not None:
            emit(current, trips, revenue)
        current = day
        trips = count
        revenue = total

if current is not None:
    emit(current, trips, revenue)