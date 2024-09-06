from common import get_queries_statistics_by_time, round_numeric_columns, plot_one_meas
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os

alpha = 0.5
window_duration = 50000
slide_duration = 10000
#available_time = slide_duration / 10
number_of_dimensions = 2
state_records_percentage = 0.05
percentage_records = state_records_percentage#0.01
tolerance = 1e-8
frequency = 10

naiveAlgorithm = "\\texttt{NAIVE}"
S1Alg = "\\texttt{A-S1}"
SEAlg = "\\texttt{A-SE}"
SKEAlg = "\\texttt{A-SKE}"

markers = {
    naiveAlgorithm: None,
    S1Alg: 'o',
    SEAlg: None,
    SKEAlg: 'x'
}

line_styles = {
  naiveAlgorithm: "-.",
  S1Alg: "-",
  SEAlg: "-",
  SKEAlg: "-"
}

colors = {
  naiveAlgorithm: "black",
  S1Alg: "blue",
  SEAlg: "orange",
  SKEAlg: "green"
}

def algorithm_name(row):
    if row["isNaive"]:
        return naiveAlgorithm
    if row["single"] and row["knapsack"] == False:
        return S1Alg
    return SEAlg if row["knapsack"] == False else SKEAlg


def process_df(df):
    df = df[
        (df['alpha'] == alpha) &
        ((np.isclose(df['stateCapacity'], state_records_percentage, atol=tolerance)) | (df["isNaive"] == True)) &
        (df["windowDuration"] == window_duration) &
        (df["slideDuration"] == slide_duration) &
        (df["k"] == number_of_dimensions) &
        (df["maximumQueryCardinalityPercentage"] == percentage_records) &
        (df["frequency"] == frequency)
       # (df["inputFile"].str.contains("knapsack") == False)
    ]
    df["algorithm_name"] = df.apply(lambda r: algorithm_name(r), axis = 1)
    return df

grouping_columns = ["dataset", "inputFile", "algorithm_name"]
df = get_queries_statistics_by_time(process_df, grouping_columns)
df["VM"] = df.apply(lambda r: (r['queryCardinalityLastPane_sum'] / (r["lastPaneRecords_max"] * state_records_percentage) if r["lastPaneRecords_max"] > 0 else 0) if r["algorithm_name"] == naiveAlgorithm else r["VM"], axis = 1)

y = "support_sel_avg"
y_label = "$supp(w_i.q^*)$"
x = "time"
graph_lines = "algorithm_name"
x_label = "Time"
lines_label = "Algorithm"

plot_one_meas(df[df['dataset'] == "$D_{syn-k}$"], x, x_label, y, y_label, "dataset", graph_lines, lines_label, "inputFile", "change_sel", markers, line_styles, colors)

aggregations = {
                'score_sel_avg': 'mean',
                #'support_sel_avg': 'mean',
                #'change_sel': 'mean',
                'SM': 'mean',
                'QM': 'mean',
                'TM': 'mean',
                'VM': 'mean',
                'total_time': 'mean'
}

result_aggr_file = df.groupby(["dataset", "inputFile", "algorithm_name"]).agg(aggregations).reset_index()
result_aggr_file["QM"] = result_aggr_file.apply(lambda r: 1 if r["algorithm_name"] == naiveAlgorithm else r["QM"], axis = 1)
result_aggr_file["SM"] = result_aggr_file.apply(lambda r: 1 if r["algorithm_name"] == naiveAlgorithm else r["SM"], axis = 1)

result_aggr = result_aggr_file.groupby(["dataset", "algorithm_name"]).agg(aggregations).reset_index()

result_aggr.to_csv("test/tables/6.5_stats.csv", index=False)