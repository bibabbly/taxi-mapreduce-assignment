import sys

current_zone = None
trips = 0
total_fare = 0.0
total_tip = 0.0
total_revenue = 0.0
total_distance = 0.0


def emit(zone, trips, fare, tip, revenue, distance):
    avg_fare = fare / trips if trips else 0
    avg_distance = distance / trips if trips else 0
    print(f"{zone}\t{trips}\t{fare:.2f}\t{tip:.2f}\t"
          f"{revenue:.2f}\t{avg_fare:.2f}\t{avg_distance:.2f}")


for line in sys.stdin:
    fields = line.strip().split("\t")
    if len(fields) != 6:
        continue
    try:
        zone = fields[0]
        count = int(fields[1])
        fare = float(fields[2])
        tip = float(fields[3])
        revenue = float(fields[4])
        distance = float(fields[5])
    except ValueError:
        continue

    if zone == current_zone:
        trips += count
        total_fare += fare
        total_tip += tip
        total_revenue += revenue
        total_distance += distance
    else:
        if current_zone is not None:
            emit(current_zone, trips, total_fare, total_tip,
                 total_revenue, total_distance)
        current_zone = zone
        trips = count
        total_fare = fare
        total_tip = tip
        total_revenue = revenue
        total_distance = distance

if current_zone is not None:
    emit(current_zone, trips, total_fare, total_tip,
         total_revenue, total_distance)