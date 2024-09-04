from common import get_queries_statistics_by_time, round_numeric_columns, base_path, set_font
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

alpha = 0.5
window_duration = 50000
slide_duration = 10000
tolerance = 1e-8
number_of_dimensions = 2
state_records_percentage = 0.05
percentage_records = state_records_percentage#0.01
measures = ["SM", "QM", "TM", "VM"]
datasets = ["synthetic/full_sim"]


def process_df_freq(df):
    return df[
                (df["isNaive"] == False) &
                (df["knapsack"] == True) &
                (df["alpha"] == alpha) &
                (df["windowDuration"] == window_duration) &
                (df["slideDuration"] == slide_duration) &
                (df["pattern.numberOfDimensions"] == number_of_dimensions) &
                (np.isclose(df['state_records_percentage'], state_records_percentage, atol=tolerance)) &
                (df["percentageOfRecordsInQueryResults"] == percentage_records) &
                (df["inputFile"] == "full_sim.csv") &
                (np.isclose(df['state_records_percentage'], df["percentageOfRecordsInQueryResults"], atol=tolerance))
            ]

def process_df_panes(df):
    return df[
                (df["isNaive"] == False) &
                (df["knapsack"] == True) &
                (df["alpha"] == alpha) &
                (df["windowDuration"] == window_duration) &
                (df["frequency"] == 10) &
                (df["pattern.numberOfDimensions"] == number_of_dimensions) &
                (df["inputFile"] == "full_sim.csv") &
                (np.isclose(df['state_records_percentage'], state_records_percentage, atol=tolerance)) &
                (df["percentageOfRecordsInQueryResults"] == percentage_records) &
                (np.isclose(df['state_records_percentage'], df["percentageOfRecordsInQueryResults"], atol=tolerance))
            ]

grouping_columns_frequency = ["inputFile", "frequency"]
frequencies_df = get_queries_statistics_by_time(process_df_freq, grouping_columns_frequency, datasets)
frequencies_df = frequencies_df[frequencies_df["lastPaneRecords_max"] > 0]

grouping_columns_pane = ["inputFile", "slideDuration"]
panes_df = get_queries_statistics_by_time(process_df_panes, grouping_columns_pane, datasets)
panes_df = panes_df[panes_df["lastPaneRecords_max"] > 0]

def aggregate(df, columns):
    return df.groupby(columns).agg({
                  'TM': 'mean',
                  'VM': 'mean',
                  'SM': 'mean',
                  'QM': 'mean'
              }).reset_index()

result_frequency = aggregate(frequencies_df, grouping_columns_frequency)
print(result_frequency)
result_frequency['frequency'] = result_frequency['frequency'].apply(lambda x: int(x) if x < 10 else round(x / 10) * 10) * 1000

result_panes = aggregate(panes_df, grouping_columns_pane)
print(result_panes)

x1 = "frequency"
x2 = "slideDuration"
x1_label = "records/s"
x2_label = "$w\_per$"

set_font()
plt.clf()
fig, (ax1, ax2) = plt.subplots(nrows=1, ncols=2, figsize=(20, 6))
handles = []
labels = []
for ax in [ax1, ax2]:
    ax.set_xlabel(x1_label if ax == ax1 else x2_label)
    ax.set_ylabel("Avg(metric)")
    #change x ticks to integer
    df = result_frequency if ax == ax1 else result_panes
    x = x1 if ax == ax1 else x2

    #x_values = df[x] if ax == ax1 else df[x].astype(str).unique()
    #x_values_plot = x_values if ax == ax1 else [x for x in range(len(x_values))]
    x_values = df[x].astype(int).astype(str).unique()
    x_values_plot = [x for x in range(len(x_values))]

    for m in measures:
        line, = ax.plot(x_values_plot, df[m], marker = 'o', linestyle='-', label=m)
        if m not in labels:
            handles.append(line)
            labels.append(m)
    #if ax == ax1:
    #    ax.set_xticks([int(v) for v in df[x].unique()])
    #else:
    ax.set_xticks(x_values_plot)
    ax.set_xticklabels(x_values)

    #plot y ticks from 0 to 1 with step 0.2
    ax.set_yticks(np.arange(0, 1.2, 0.2))

fig.legend(handles, labels, bbox_to_anchor=(0.3, .9), loc=3, ncol=len(handles), borderaxespad=0.)

plt.tight_layout()
fig.subplots_adjust(top=0.89)

plt.savefig(f"test/graphs/fig_setting_times.png")
plt.close()

