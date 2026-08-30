import sys

LABELS = {
    "1": "Credit card",
    "2": "Cash",
    "3": "No charge",
    "4": "Dispute",
    "5": "Unknown",
    "6": "Voided trip",
}

current = None
trips = 0
total_fare = 0.0
total_tip = 0.0
total_revenue = 0.0


def emit(ptype, trips, fare, tip, revenue):
    label = LABELS.get(ptype, "Other")
    avg_fare = fare / trips if trips else 0
    avg_tip = tip / trips if trips else 0
    print(f"{ptype}\t{label}\t{trips}\t{revenue:.2f}\t"
          f"{avg_fare:.2f}\t{avg_tip:.2f}\t{tip:.2f}")


for line in sys.stdin:
    fields = line.strip().split("\t")
    if len(fields) != 5:
        continue
    try:
        ptype = fields[0]
        count = int(fields[1])
        fare = float(fields[2])
        tip = float(fields[3])
        revenue = float(fields[4])
    except ValueError:
        continue

    if ptype == current:
        trips += count
        total_fare += fare
        total_tip += tip
        total_revenue += revenue
    else:
        if current is not None:
            emit(current, trips, total_fare, total_tip, total_revenue)
        current = ptype
        trips = count
        total_fare = fare
        total_tip = tip
        total_revenue = revenue

if current is not None:
    emit(current, trips, total_fare, total_tip, total_revenue)