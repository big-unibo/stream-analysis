from common import get_queries_statistics_by_time, round_numeric_columns
import pandas as pd
import numpy as np

alpha = 0.5
window_duration = 50000
slide_duration = 10000
state_records_percentage = 0.05
percentage_records = state_records_percentage#0.01
tolerance = 1e-8
frequency = 10

def process_df(df):
    return df[
                (df["isNaive"] == False) &
                (df["knapsack"] == True) &
                (df["alpha"] == alpha) &
                (np.isclose(df['state_records_percentage'], state_records_percentage, atol=tolerance)) &
                (df["windowDuration"] == window_duration) &
                (df["slideDuration"] == slide_duration) &
                (df["percentageOfRecordsInQueryResults"] == percentage_records) &
                (df["inputFile"].str.contains("knapsack") == False) &
                (df["frequency"] == frequency)
            ]

grouping_columns=["pattern.numberOfDimensions", "inputFile", "dataset"]
result = get_queries_statistics_by_time(process_df, grouping_columns)
#filter out from result where dataset is D_{{real}_2} pattern.numberOfDimensions = 3 and time_choose_queries > 1000
#result = result[~((result["dataset"] == "$D_{{real}_2}$") & (result["pattern.numberOfDimensions"] == 3) & (result["time_choose_queries"] > 1000))]

def aggregate(df, columns):
    return df.groupby(columns).agg(
               total_time = ('total_time', 'mean'),
               time_score = ('time_score', 'mean'),
               time_choose_queries = ('time_choose_queries', 'mean'),
               time_execute_queries = ('time_execute_queries', 'mean'),
               total_queries = ('total_queries', 'mean'),
               attributes_avg = ('attributes_avg', 'mean'),
               measures_avg = ('measures_avg', 'mean'),
           ).reset_index()

time_statistics_inputFile = aggregate(result, grouping_columns)
time_statistics_aggr = aggregate(time_statistics_inputFile, ["pattern.numberOfDimensions", "dataset"])

#change time_score, time_choose_queries and time_execute_queries to percentage with respect to total_time
times_cols = ["time_score", "time_choose_queries", "time_execute_queries"]
for col in times_cols:
    time_statistics_aggr[col] = time_statistics_aggr[col] / time_statistics_aggr["total_time"] * 100

time_statistics_aggr["diff"] = (100 - time_statistics_aggr[times_cols].sum(axis=1))/3
for col in times_cols:
    time_statistics_aggr[col] = time_statistics_aggr[col] + time_statistics_aggr["diff"]

time_statistics_aggr = round_numeric_columns(time_statistics_aggr, decimals=2)

for col in times_cols:
    #add the percentage sign
    print(f"Average {col} over total time", time_statistics_aggr[col].mean())
    time_statistics_aggr[col] = time_statistics_aggr[col].astype(str) + "\%"

print(time_statistics_aggr[[
"pattern.numberOfDimensions",
"dataset", "total_time", "time_score",
"time_choose_queries", "time_execute_queries", "total_queries"
]].to_latex(index=False, escape=False))

time_statistics_aggr.to_csv("debug/analysis/6.4.1_time_aggr.csv", index=False)
