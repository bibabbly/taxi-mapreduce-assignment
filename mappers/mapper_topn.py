import sys

# Stage 2 mapper: re-key by revenue so the shuffle sorts on it.
# Input is stage 1 output: zone, trips, fare, tip, revenue, avg_fare, avg_dist

for line in sys.stdin:
    fields = line.strip().split("\t")
    if len(fields) != 7:
        continue
    try:
        zone = fields[0]
        trips = int(fields[1])
        revenue = float(fields[4])
    except ValueError:
        continue
    # zero-pad so lexicographic shuffle order matches numeric order
    print(f"{revenue:016.2f}\t{zone}\t{trips}")