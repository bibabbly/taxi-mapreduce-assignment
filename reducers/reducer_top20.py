import sys

TOP_N = 20
window = []

for line in sys.stdin:
    fields = line.strip().split("\t")
    if len(fields) != 3:
        continue
    window.append(fields)
    if len(window) > TOP_N:
        window.pop(0)

print("rank\troute\ttrips\ttotal_revenue")
for rank, (revenue, route, trips) in enumerate(reversed(window), start=1):
    print(f"{rank}\t{route}\t{trips}\t{float(revenue):.2f}")