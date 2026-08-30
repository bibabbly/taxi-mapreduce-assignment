import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import os

os.makedirs("charts", exist_ok=True)


def read_rows(path):
    rows = []
    with open(path) as f:
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if parts and parts[0]:
                rows.append(parts)
    return rows


def save(fig, name):
    fig.tight_layout()
    fig.savefig(f"charts/{name}.png", dpi=150)
    plt.close(fig)
    print(f"charts/{name}.png")


# 1 - trips by hour
rows = read_rows("results/hourly_demand.txt")
hours = [r[0] for r in rows]
trips = [int(r[1]) for r in rows]
fig, ax = plt.subplots(figsize=(10, 5))
ax.bar(hours, trips, color="#c8553d")
ax.set_title("NYC Yellow Taxi Trips by Hour (Jan-Mar 2024)")
ax.set_xlabel("Hour of day")
ax.set_ylabel("Trips")
save(fig, "01_trips_by_hour")

# 2 - trips by day of week
rows = read_rows("results/daily_demand.txt")
days = [r[0][:3] for r in rows]
trips = [int(r[1]) for r in rows]
fig, ax = plt.subplots(figsize=(9, 5))
ax.bar(days, trips, color="#588157")
ax.set_title("Trips by Day of Week")
ax.set_ylabel("Trips")
save(fig, "02_trips_by_day")

# 3 - top 10 pickup zones by revenue
rows = read_rows("results/top10_revenue.txt")[1:]
zones = [f"Zone {r[1]}" for r in rows][::-1]
revenue = [float(r[3]) / 1e6 for r in rows][::-1]
fig, ax = plt.subplots(figsize=(9, 6))
ax.barh(zones, revenue, color="#3d5a80")
ax.set_title("Top 10 Pickup Zones by Revenue")
ax.set_xlabel("Revenue (millions USD)")
save(fig, "03_top10_zones_revenue")

# 4 - revenue by payment method
rows = read_rows("results/payment_analysis.txt")
labels = [r[1] for r in rows]
revenue = [float(r[3]) / 1e6 for r in rows]
fig, ax = plt.subplots(figsize=(8, 5))
ax.bar(labels, revenue, color="#8a5a44")
ax.set_title("Revenue by Payment Method")
ax.set_ylabel("Revenue (millions USD)")
save(fig, "04_revenue_by_payment")

# 5 - trips by distance category
rows = read_rows("results/distance_categories.txt")
cats = [r[0].split("_", 1)[1] for r in rows]
trips = [int(r[1]) for r in rows]
fig, ax = plt.subplots(figsize=(9, 5))
ax.bar(cats, trips, color="#6a4c93")
ax.set_title("Trips by Distance Category")
ax.set_ylabel("Trips")
save(fig, "05_trips_by_distance")

# 6 - average fare vs distance category
avg_fare = [float(r[5]) for r in rows]
fig, ax = plt.subplots(figsize=(9, 5))
ax.plot(cats, avg_fare, marker="o", color="#bc4749", linewidth=2)
ax.set_title("Average Fare by Distance Category")
ax.set_ylabel("Average fare (USD)")
ax.grid(alpha=0.3)
save(fig, "06_fare_vs_distance")

# 7 - data quality
rows = read_rows("results/anomaly_detection.txt")
d = {r[0]: int(r[1]) for r in rows}
total = d.pop("TOTAL_RECORDS")
clean = d.pop("CLEAN")
issues = sorted(d.items(), key=lambda x: x[1], reverse=True)[:6]
names = [k.replace("_", " ").title() for k, _ in issues][::-1]
counts = [v for _, v in issues][::-1]
fig, ax = plt.subplots(figsize=(9, 5))
ax.barh(names, counts, color="#7f4f24")
ax.set_title(f"Data Quality Issues ({total-clean:,} of {total:,} records affected)")
ax.set_xlabel("Records flagged")
save(fig, "07_data_quality")

print("\nDone.")