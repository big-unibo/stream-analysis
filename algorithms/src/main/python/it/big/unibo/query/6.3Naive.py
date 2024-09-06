from common import get_complete_stats_dataframe, round_numeric_columns
import pandas as pd

alpha = 0.5
window_duration = 50000
slide_duration = 10000
available_time = slide_duration / 10
number_of_dimensions = 2
percentage_records = 1 #take naive execution with 100% of records
space_available = slide_duration * 0.05
frequency = 10

def process_df(df):
    return df[
                   (df["isNaive"] == True) &
                   (df["executed"] == True) &
                   (df["alpha"] == alpha) &
                   (df["windowDuration"] == window_duration) &
                   (df["slideDuration"] == slide_duration) &
                   (df["k"] == number_of_dimensions) &
                   (df["maximumQueryCardinalityPercentage"] == percentage_records) &
                   (df["inputFile"].str.contains("knapsack") == False) &
                   (df["frequency"] == frequency)
               ]

df=get_complete_stats_dataframe(process_df)

result = df.groupby(["time", "inputFile", "dataset"]).agg(
    queryCardinalityLastPane_sum=('queryCardinalityLastPane','sum'),
    totalTime_max=('totalTime','max'),
).reset_index()

result['time_usage'] = result["totalTime_max"] / available_time
result['space_usage'] = result["queryCardinalityLastPane_sum"] / space_available

def aggregate(df, columns):
    return df.groupby(columns).agg(
        time_usage = ('time_usage', 'mean'),
        space_usage = ('space_usage', 'mean'),
    ).reset_index()


result_file = aggregate(result, ["inputFile", "dataset"])
result_aggr = aggregate(result_file, ["dataset"])
result_aggr = round_numeric_columns(result_aggr)

for col in ["time_usage", "space_usage"]:
    #add the percentage sign
    result_aggr[col] = 100 + result_aggr[col]
    result_aggr[col] =result_aggr[col].astype(str) + "\%"

result_df = result_aggr[["dataset", "time_usage", "space_usage"]]
print(result_df.to_latex(index=False, escape=False))
result_df.to_csv("test/tables/6.3_naive.csv", index=False)

