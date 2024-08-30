from common import get_complete_stats_dataframe
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

df = get_complete_stats_dataframe(lambda df: df[df["executed"] == True])
df["timeDiff"] = abs(df["estimatedTime"] - df["lastPaneExecutionTime"])
#difference between estimatedTime and lastPaneExecutionTime
df["timeDiffPercentage"] = df["timeDiff"] / df["lastPaneExecutionTime"] * 100
group_dimensions = ['slideDuration', 'dataset', 'pattern.numberOfDimensions']
grouped = df.groupby(group_dimensions).agg(
    timeDiffPercentage_mean=('timeDiffPercentage', 'mean'),
    lastPaneExecutionTime_stddev=('lastPaneExecutionTime', 'std'),
    lastPaneExecutionTime_var=('lastPaneExecutionTime', 'var'),
).reset_index()

print(grouped)
#calculate mean of lastPaneExecutionTime_stddev
print(f"Mean of lastPaneExecutionTime_stddev: {grouped['lastPaneExecutionTime_stddev'].mean()}")
#calculate mean of lastPaneExecutionTime_var
print(f"Mean of lastPaneExecutionTime_var: {grouped['lastPaneExecutionTime_var'].mean()}")

mae = np.mean(np.abs(df['lastPaneExecutionTime'] - df['estimatedTime']))
print(f'Errore Assoluto Medio (MAE): {mae}')

mse = np.mean((df['lastPaneExecutionTime'] - df['estimatedTime'])**2)
print(f'Errore Quadratico Medio (MSE): {mse}')

rmse = np.sqrt(mse)
print(f'Radice dell\'Errore Quadratico Medio (RMSE): {rmse}')

mape = np.mean(np.abs((df['lastPaneExecutionTime'] - df['estimatedTime']) / df['lastPaneExecutionTime'])) * 100
print(f'Errore Percentuale Medio Assoluto (MAPE): {mape}%')