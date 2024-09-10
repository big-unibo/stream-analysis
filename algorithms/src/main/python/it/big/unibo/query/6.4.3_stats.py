from common import get_queries_statistics_by_time, round_numeric_columns, set_font
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

window_duration = 50000
slide_duration = 10000
number_of_dimensions = 2
state_records_percentage = 0.05
percentage_records = state_records_percentage#0.01
tolerance = 1e-8
frequency = 10

def process_df(df):
    return df[
        (df["isNaive"] == False) &
        (df["knapsack"] == True) &
        (np.isclose(df['stateCapacity'], state_records_percentage, atol=tolerance)) &
        (df["windowDuration"] == window_duration) &
        (df["slideDuration"] == slide_duration) &
        (df["k"] == number_of_dimensions) &
        (df["maximumQueryCardinalityPercentage"] == percentage_records) &
        (df["inputFile"].str.contains("knapsack") == False) &
        (df["frequency"] == frequency)
    ]

grouping_columns = ["dataset", "inputFile", "alpha"]

result = get_queries_statistics_by_time(process_df, grouping_columns)

x = "time"
graph_lines = "alpha"
x_label = x
lines_label = graph_lines
supp_meas = "support_sel_avg"

aggregations = {
                   supp_meas: 'mean',
                   'change_sel': 'mean'
               }
aggr_col = ["dataset", "alpha", "time"]
result_file = result.groupby(aggr_col + ["inputFile"]).agg(aggregations).reset_index()

def plotPaper(df):
    set_font()
    m1 = supp_meas
    m2 = 'change_sel'
    detail = 'dataset'
    x1 = 'time'
    graph_lines = "alpha"
    lines_label = graph_lines
    d = "$D_{syn}$"
    df_reduced = df[(df[detail] == d) & (df[x1] > 0)]
    #get unique inputFile
    inputFiles = df_reduced["inputFile"].unique()
    for f in inputFiles:
        df_graph = df_reduced[df_reduced["inputFile"] == f]
        bar_width = 0.6
        bar_space = 0.2
        positions = np.arange(len(df_graph[graph_lines].unique())) * (bar_width + bar_space)

        plt.clf()
        fig, (ax1, ax2) = plt.subplots(nrows=1, ncols=2, figsize=(12, 6))
        for i, v in enumerate(df_graph[graph_lines].unique()):
            subset = df_graph[df_graph[graph_lines] == v]
            ax1.plot(subset[x1], subset[m1], marker = 'o', linestyle='-', label=v)
            #ax1.bar(positions[i], subset[m1].mean(), bar_width, label=v)
            ax2.bar(positions[i], subset[m2].mean(), bar_width, label=v)
        ax1.set_xlabel("Time")
        ax2.set_xlabel(f"$\\alpha$")
        ax2.set_ylabel("Avg(Query changes)")
        ax1.set_ylabel("Best query support")
        ax1.set_ylim(0, 1.02)  # Set y-axis limits to 0-1
        ax1.set_xticks([1] + [x for x in range(5, subset[x1].max() + 5, 5)])
        ax2.set_xticks(positions)
        ax2.set_xticklabels(df_graph[graph_lines].unique())
        #ax1.grid(True)
        # Adjust layout and display the plot
        ax1.legend(title = f"$\\alpha$")
        plt.tight_layout()

        plt.savefig(f"test/graphs/fig_alpha_beta_{f}.png")
        plt.close()

plotPaper(result_file)
