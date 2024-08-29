from common import get_complete_stats_dataframe
import pandas as pd
import matplotlib.pyplot as plt

df = get_complete_stats_dataframe(lambda df: df[df["executed"] == True])

# Binning the 'lastPaneRecords' column
bins = range(0, df['lastPaneRecords'].max() + 2500, 2500)
labels = [f"{i}-{i+2500}" for i in bins[:-1]]
df['records_bins'] = pd.cut(df['lastPaneRecords'], bins=bins, labels=labels)
df = df.dropna(subset=['lastPaneRecords'])

group_dimensions = ['slideDuration', 'dataset', 'pattern.numberOfDimensions'] # consider also "measures"?

# Function to compute percentiles for each group
def compute_percentiles(group):
    p10 = group['lastPaneExecutionTime'].quantile(0.1)
    p90 = group['lastPaneExecutionTime'].quantile(0.9)
    return pd.Series({'p10': p10, 'p90': p90})

percentiles = df.groupby(group_dimensions).apply(compute_percentiles).reset_index()
df = df.merge(percentiles, on=group_dimensions)
df = df[(df['lastPaneExecutionTime'] >= df['p10']) & (df['lastPaneExecutionTime'] <= df['p90'])].reset_index()
grouped_df = df.groupby(group_dimensions)["lastPaneExecutionTime"].agg(
    variance='var',
    mean="mean",
    std="std",
    min="min",
    max="max",
    count="count",
).reset_index()

grouped_df = grouped_df[grouped_df['count'] > 0]

# Display the result
print(grouped_df)
grouped_df.to_csv(f"debug/analysis/6.1query_times.csv", index=False)

#grouped = df.groupby(group_dimensions)['lastPaneExecutionTime'].apply(list)
#grouped = grouped.dropna()
# Create the box plot
#plt.figure(figsize=(10, 6))
#plt.boxplot(grouped, labels=grouped.index)

# Set the title and labels
#plt.title('Distribution of lastPaneExecutionTime by Number of Dimensions')
#plt.xlabel('Number of Dimensions')
#plt.ylabel('lastPaneExecutionTime')

# Show the plot
#plt.show()