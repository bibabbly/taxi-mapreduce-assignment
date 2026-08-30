import sys

for line in sys.stdin:
    parts = line.strip().split(",")
    if len(parts) != 10:
        print("MALFORMED_ROW\t1")
        continue

    try:
        passengers = float(parts[2]) if parts[2] else -1
        distance = float(parts[3])
        fare = float(parts[7])
        tip = float(parts[8])
        total = float(parts[9])
    except ValueError:
        print("UNPARSEABLE_NUMERIC\t1")
        continue

    clean = True

    if distance <= 0:
        print("ZERO_OR_NEG_DISTANCE\t1")
        clean = False
    elif distance > 200:
        print("EXTREME_DISTANCE\t1")
        clean = False

    if fare < 0:
        print("NEGATIVE_FARE\t1")
        clean = False
    elif fare > 1000:
        print("EXTREME_FARE\t1")
        clean = False

    if total < 0:
        print("NEGATIVE_TOTAL\t1")
        clean = False

    if tip < 0:
        print("NEGATIVE_TIP\t1")
        clean = False

    if passengers < 0:
        print("MISSING_PASSENGER_COUNT\t1")
        clean = False
    elif passengers == 0:
        print("ZERO_PASSENGERS\t1")
        clean = False
    elif passengers > 6:
        print("EXCESSIVE_PASSENGERS\t1")
        clean = False

    if distance > 0 and fare > 0:
        per_mile = fare / distance
        if per_mile > 100:
            print("EXTREME_FARE_PER_MILE\t1")
            clean = False

    if clean:
        print("CLEAN\t1")

    print("TOTAL_RECORDS\t1")