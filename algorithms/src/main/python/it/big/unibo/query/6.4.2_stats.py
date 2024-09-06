from common import get_queries_statistics_by_time, round_numeric_columns, base_path, set_font
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

alpha = 0.5
window_duration = 50000
slide_duration = 10000
tolerance = 1e-8
number_of_dimensions = 2
frequency = 10

def process_df(df):
    return df[
                (df["isNaive"] == False) &
                (df["knapsack"] == True) &
                (df["alpha"] == alpha) &
                (df["windowDuration"] == window_duration) &
                (df["slideDuration"] == slide_duration) &
                (df["k"] == number_of_dimensions) &
                (df["inputFile"].str.contains("knapsack") == False) &
                (np.isclose(df['stateCapacity'], df["maximumQueryCardinalityPercentage"], atol=tolerance)) &
                (df["frequency"] == frequency)
            ]

grouping_columns = ["dataset", "inputFile", "maximumQueryCardinalityPercentage", "stateCapacity"]
result = get_queries_statistics_by_time(process_df, grouping_columns)

def aggregate(df, columns):
    return df.groupby(columns).agg({
                  'TM': 'mean',
                  'VM': 'mean',
                  'SM': 'mean',
                  'QM': 'mean'
              }).reset_index()

result_file = aggregate(result, grouping_columns)

measures = ["SM", "QM", "TM", "VM"]
x = "stateCapacity"
graph_lines = "maximumQueryCardinalityPercentage"
x_label = f"$\eta$"
lines_label = "Max result card \%"

result_dataset = aggregate(result_file, ["dataset", "maximumQueryCardinalityPercentage", "stateCapacity"])

def plotPaper(df):
    set_font()
    df_reduced = df[df['dataset'] == "$D_{syn}$"]
    plt.clf()
    plt.figure(figsize=(10, 6))
    for m in measures:
        plt.plot(df_reduced[x], df_reduced[m], marker = 'o', linestyle='-', label=m)

    plt.xlabel(x_label)
    plt.ylabel("Avg(metric)")
    #plot x ticks considering distinct values
    plt.xticks(df_reduced[x].unique())
    #plot y ticks from 0 to 1 with step 0.2
    plt.yticks(np.arange(0, 1.2, 0.2))
    plt.legend(loc = "upper left", bbox_to_anchor=(-0.05,1.25), ncol = len(measures))
    plt.tight_layout()

    plt.savefig(f"test/graphs/fig_state_records_percentage.png")
    plt.close()

plotPaper(result_dataset)
