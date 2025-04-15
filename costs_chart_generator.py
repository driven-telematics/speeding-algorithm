import pandas as pd
import matplotlib.pyplot as plt

# Load CSV
df = pd.read_csv("./AWS_Costs/aws_costs.csv")
df = df[1:].reset_index(drop=True)
data = df.iloc[:, 1:-4]
total_montly_cost_data = df.iloc[:, -1]

row_index = 5
costs = data.iloc[row_index].astype(float)
total_monthly_cost = total_montly_cost_data.iloc[row_index]
labels = data.columns
chart_date = df.iloc[row_index]['Service']
total = costs.sum()

# Group small slices (<1%) into "Other"
grouped_costs = []
grouped_labels = []
other_total = 0.0

for label, cost in zip(labels, costs):
    pct = cost / total * 100
    if pct < 1:
        other_total += cost
        print(f"Grouping {label} (${cost:.2f}) into 'Other'")
    else:
        grouped_costs.append(cost)
        grouped_labels.append(label)

# Add "Other" if needed
if other_total > 0:
    grouped_costs.append(other_total)
    grouped_labels.append("Other")

grouped_total = sum(grouped_costs)

# Custom formatter for slice labels (bold dollar amount)
def format_autopct(pct):
    value = pct * grouped_total / 100.0
    return f'{pct:.1f}%\n${value:.2f}'

# Pie chart
plt.figure(figsize=(12, 10))
wedges, texts, autotexts = plt.pie(
    grouped_costs,
    labels=grouped_labels,
    # autopct=format_autopct, # Uncomment for % and Dollar Value
    autopct='%1.1f%%',
    startangle=140
)

# Custom legend
legend_labels = [
    f"{label} – ${(cost / 17000):.4f}"
    for label, cost in zip(grouped_labels, grouped_costs)
]
plt.legend(wedges, legend_labels, title="AWS Services Cost per User", loc="center left", bbox_to_anchor=(1, 0.75))
plt.text(
    1.4, 1.3,
    f"Total Monthly Cost: ${total_monthly_cost:.2f}",
    fontsize=12,
    fontweight='bold'
)
plt.text(
    1.4, 1.2,
    f"Total Cost per User: ${(total_monthly_cost / 17000):.2f}",
    fontsize=12,
    fontweight='bold'
)
plt.title(f"AWS Cost Breakdown by Service per User – {chart_date}")
plt.axis('equal')
plt.tight_layout()

# Save
output_path = f"./AWS_Costs/aws_costs_chart_{chart_date}.png"
plt.savefig(output_path, dpi=300, bbox_inches='tight')
plt.show()
