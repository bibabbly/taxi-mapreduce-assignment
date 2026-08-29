import sys

TOP_N = 10
window = []

# Input arrives sorted ascending by revenue, so the last N lines
# are the top N. Holding only N rows keeps memory constant.

for line in sys.stdin:
    fields = line.strip().split("\t")
    if len(fields) != 3:
        continue
    window.append(fields)
    if len(window) > TOP_N:
        window.pop(0)

print("rank\tzone\ttrips\ttotal_revenue")
for rank, (revenue, zone, trips) in enumerate(reversed(window), start=1):
    print(f"{rank}\t{zone}\t{trips}\t{float(revenue):.2f}")