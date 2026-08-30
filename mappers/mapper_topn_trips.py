import sys

for line in sys.stdin:
    fields = line.strip().split("\t")
    if len(fields) != 7:
        continue
    try:
        route = fields[0]
        trips = int(fields[1])
        revenue = float(fields[4])
    except ValueError:
        continue
    print(f"{trips:012d}\t{route}\t{revenue:.2f}")