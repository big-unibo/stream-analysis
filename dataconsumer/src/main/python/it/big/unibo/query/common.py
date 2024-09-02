import pandas as pd
import matplotlib.pyplot as plt
import sys
import os
import numpy as np
import math

simulation_columns = [
    "alpha", "beta", "windowDuration", "slideDuration", "pattern.numberOfDimensions",
    "approximateBits", "approximate", "percentageOfRecordsInQueryResults",
    "inputFile", "frequency", "availableTime"
]

algorithm_columns = [
     "knapsack", "state_records_percentage", "isNaive", "single"
]

configuration_columns = simulation_columns + algorithm_columns

def from_log_factor_to_percentage(log_factor, pane_records):
    try:
        return math.pow(math.log(pane_records), log_factor) / pane_records
    except (OverflowError, ValueError):
        return float('inf')

def get_complete_stats_dataframe(process_df, datasets = ["synthetic"]):
    dataframes = []
    #process the complete stats dataframe with the given function and concat all the dataframes
    def processing_function(base, base_dir):
        print(f"Processing {base}")
        dataframes.append(process_df(get_stats_df(base)))

    for d in datasets:
        process_directory(os.path.join(base_path, d), d, processing_function, "stats.csv")

    df = pd.concat(dataframes, ignore_index=True)

    #filter out when $D_{syn-k}$ and time > 10, keep the other datasets
    df = df[~((df["dataset"] == "$D_{syn-k}$") & (df["time"] > 10))]
    return df

# Function to round numeric columns
def round_numeric_columns(df, decimals=2):
    for col in df.select_dtypes(include='number'):
        df[col] = df[col].round(decimals)
    return df

def sanitize_label(str):
    return str.replace(".csv", "").replace(" ", "_").replace(",", "_").replace("(", "_").replace(")", "_").replace("\\", "").replace("/", "").replace(",", "_")

def get_reduced_in(row):
    res = row['inputFile'].split('/')[-1].split('\\')[-1]
    if res == "output.csv":
        arr = row['inputFile'].split('/')
        if(len(arr) == 1):
            arr = row['inputFile'].split('\\')
            res = arr[-2]
        else:
            res = arr[-2]
    return res

def get_configuration_string(row):
    if type(row) == tuple:
        row = pd.Series(row, index=configuration_columns)
    reducedIn = get_reduced_in(row)
    #from a row with configuration column return a string with the configuration
    if row["isNaive"]:
        str = f"NAIVE_alpha={row['alpha']},beta={row['beta']},wd={row['windowDuration']},sd={row['slideDuration']},dims={row['pattern.numberOfDimensions']},pqr={row['percentageOfRecordsInQueryResults']:.3f},in={reducedIn},f={row['frequency']}"
    else:
        str = f"alpha={row['alpha']},beta={row['beta']},wd={row['windowDuration']},sd={row['slideDuration']},dims={row['pattern.numberOfDimensions']},pqr={row['percentageOfRecordsInQueryResults']:.3f},sp={row['state_records_percentage']:.3f},kn={row['knapsack']},s={row['single']},in={reducedIn},f={row['frequency']}"

    return sanitize_label(str)

def get_simulation_string(row):
    if type(row) == tuple:
        row = pd.Series(row, index=simulation_columns)
    #from a row with simulation column return a string with the configuration
    reducedIn = get_reduced_in(row)
    str = f"alpha={row['alpha']},beta={row['beta']},wd={row['windowDuration']},sd={row['slideDuration']},dims={row['pattern.numberOfDimensions']},pqr={row['percentageOfRecordsInQueryResults']:.3f},in={reducedIn},f={row['frequency']}"
    str = sanitize_label(str)
    label = f"{get_path_to_store_results()}/{str}"
     #create the directory if not exists
    os.makedirs(label, exist_ok=True)
    return str

def get_algorithm_string(row):
    if type(row) == tuple:
        row = pd.Series(row, index=algorithm_columns)
    #from a row with algorithm column return a string with the configuration
    return "N" if row["isNaive"] else f"sp={row['state_records_percentage']:.3f}_kn={row['knapsack']}_s={row['single']}"

def format_selection(row):
    if row['selected'] and row['stored']:
        return "SE"
    elif row['stored']:
        return "E"
    elif row['selected'] and row['executed']:
        return "Se"
    elif row['executed']:
        return "e"
    elif row['selected']:
        return "S"
    elif pd.isna(row['SOP']):
        return None
    else:
        return "N"

def generate_line_styles(num_styles):
    """
    Generate a list of line styles to use in plots.
    """
    # Define a list of line styles
    line_styles = ['-', '--', '-.', ':']

    # Repeat line styles to cover num_styles
    repetitions = num_styles // len(line_styles) + 1
    line_styles *= repetitions

    return line_styles[:num_styles]

def get_color_map(df):
    """
    Create a color map based on the state_records_percentage and single columns of the DataFrame.
    """
    # Create a unique key by combining state_records_percentage and single
    df['composite_key'] = df.apply(lambda row: (row['state_records_percentage'], row['single']), axis=1)

    # Get unique composite keys
    unique_composite_keys = df['composite_key'].unique()

    # Generate colors for the unique composite keys
    num_colors = len(unique_composite_keys)
    colors = plt.get_cmap('tab10')(np.linspace(0, 1, num_colors))

    # Create a color map dictionary
    color_map = {key: colors[i] for i, key in enumerate(unique_composite_keys)}

    return color_map

# Function to get color for a specific combination of state_records_percentage and single
def get_color(state_records_percentage, num_queries, color_map):
    key = (state_records_percentage, num_queries)
    return color_map.get(key, None)  # Return None if the key is not found

def get_input_folder():
    """
    Get the input folder from the command line arguments.
    """
    return sys.argv[1] if len(sys.argv) > 1 else "full_sim"

def read_df(name, input_folder):
    """
    Read the CSV file with the given name and return the DataFrame.
    """
    return pd.read_csv(f"{base_path}{input_folder}/{name}.csv", sep=',', quotechar='"', decimal='.')

def get_df(name, input_folder = get_input_folder()):
    """
    Read the CSV file with the given name and return the DataFrame.
    """
    df = read_df(name, input_folder)
    df = df.sort_values(by='paneTime')
    df['time'] = df.groupby('paneTime').ngroup()
    #remove the column pane time
    df = df.drop(columns=['paneTime'])
    return df

def save_df_to_csv(df, name, index = True):
    """
    Save the DataFrame to a CSV file with the given name in the results folder.
    """
    df.to_csv(f"{get_path_to_store_results()}/{name}.csv", index=index)

def get_dataset(input_file, input_folder):
    """
    Get the dataset name from the input file.
    """
    if "knapsack" in input_file:
        return "$D_{syn-k}$"
    else:
        dataset = input_folder.split("\\")[0].split("/")[0]
        # make a case selection on the dataset
        if "synthetic" in dataset:
            return "$D_{syn}$"

def get_stats_df(input_folder = get_input_folder()):
    """
    Read the stats.csv file and return the DataFrame.
    """
    df = get_df("stats", input_folder)
    df["state_records_percentage"] = df.apply(lambda row: from_log_factor_to_percentage(row['logFactor'], row['slideDuration']), axis=1)
    # Apply the function to create the 'info' column
    df['selection'] = df.apply(format_selection, axis=1)
    df['algorithm'] = df.apply(lambda row: get_algorithm_string(row), axis=1)
    df['inputFile'] = df.apply(lambda row: get_reduced_in(row), axis=1)
    df['simulation'] = df.apply(lambda row: get_simulation_string(row), axis=1)
    df["dataset"] = df["inputFile"].apply(lambda x: get_dataset(x, input_folder))
    return df

def calculate_mean(number_string):
    """
    Calculate the mean of a string of comma-separated numbers.
    """
    # Split the string into a list of strings
    number_list = str(number_string).split(',')
    # Convert the list of strings to a list of floats
    float_list = [float(num) for num in number_list]
    # Calculate the mean of the list of floats
    mean_value = sum(float_list) / len(float_list)
    return mean_value

def get_path_to_store_results():
    """
    Get the path to store the results.
    """
    return f"test/{get_input_folder()}"

base_dir = r"./dataconsumer/src/main/python/it/big/unibo/query"
base_path = r"./test/"

def process_directory(path, base, function, file_name):
    # List all entries in the directory
    try:
        entries = os.listdir(path)
    except FileNotFoundError:
        print(f"Path {path} not found.")
        return
    except PermissionError:
        print(f"Permission denied for accessing {path}.")
        return

    subdirs = [entry for entry in entries if os.path.isdir(os.path.join(path, entry))]
    if len(subdirs) > 0 and not (file_name in entries):
        for subdir in subdirs:
            new_path = os.path.join(path, subdir)
            new_base = os.path.join(base, subdir) if base else subdir
            process_directory(new_path, new_base, function, file_name)
    elif file_name in entries:
        function(base, base_dir)
    else:
        print(f"No subdirectories found in {path} with {file_name}.")

def get_queries_statistics_by_time(process_df, grouping_columns, datasets = ["synthetic"]):
    """
    Get the statistics of queries executed by time. Considering executed, selected and total queries
    """
    df = get_complete_stats_dataframe(process_df, datasets)
    df['change'] = df['notChange'] .apply(lambda x : 0 if x == 1 else 1)
    columns = grouping_columns + ["time"]
    executed_df = df[df["stored"] == True]

    result_executed = executed_df.groupby(columns).agg(
       executed_queries=('executed','sum'),
       numberOfQueriesToExecute_max=('numberOfQueriesToExecute','max'),
       lastPaneRealRecords_sum=('lastPaneRealRecords','sum'),
       lastPaneMaxRecords_max=('lastPaneMaxRecords','max'),
       lastPaneRecords_max=('lastPaneRecords','max'),
       SOP_sum_ex=('SOP','sum'),
       SOPSupport_ex_avg=('SOPSupport','mean'),
       SOP_support_sum_ex=('SOPSupport','sum'),
       SOPFd_sum_ex=('SOPFd','sum'),
    ).reset_index()

    result_tot = df.groupby(columns).agg(
        total_queries = ('dimensions', 'size'), # total number of queries
        total_time = ('totalTime', 'max'),
        time_score = ('timeForScoreComputation', 'max'),
        time_choose_queries = ('timeForChooseQueries', 'max'),
        time_execute_queries = ('timeForQueryExecution', 'max'),
        attributes_avg = ('numberOfAttributes', 'mean'),
        measures_avg = ('measures', 'mean'),
        SOP_sum=('SOP','sum')
    )
    result_tot['extra_time'] = result_tot['total_time'] - (result_tot['time_score'] + result_tot['time_choose_queries'] + result_tot['time_execute_queries'])
    result = pd.merge(result_executed, result_tot, on=columns, how='outer')
    # Total queries is the minimum between queries executed and to execute
    result['queries'] = result[['numberOfQueriesToExecute_max', 'total_queries']].min(axis=1)
    result['TM'] = result['executed_queries'] / result['queries']
    result['VM'] = result['lastPaneRealRecords_sum'] / result['lastPaneMaxRecords_max']
    result['SM'] = result['SOP_sum_ex'] / result['SOP_sum']
    result['Support_SM'] = result['SOP_support_sum_ex'] / result['total_queries']
    result['FD_SM'] = result['SOPFd_sum_ex'] / result['total_queries']
    #query eseguite rispetto al totale delle fattibili
    result['QM'] = result['executed_queries'] / result["total_queries"]

    selected_df = df[df["selected"] == True]
    result_selected = selected_df.groupby(columns).agg(
       SOP_sel_avg=('SOP','mean'),
       SOPFd_sel_avg=('SOPFd','mean'),
       SOPSupport_sel_avg=('SOPSupport','mean'),
       SupportLastPane_sel_avg=('querySupportLastPaneReal','mean'),
       change_sel = ('change', 'max')
    ).reset_index()
    result = pd.merge(result, result_selected, on=columns, how='outer')
    # Fill NaN values with zeros only for numerical columns
    result[result.select_dtypes(include=[np.number]).columns] = result.select_dtypes(include=[np.number]).fillna(0)

    return result

font_size = 25
def set_font():
    plt.rcParams.update({
                "text.usetex": True,
                "font.family": "serif",  # Change this as per your preference
                "font.size": font_size
        })

def plot_one_meas(df, x, x_label, y, y_label, detail, graph_lines, lines_label, graphs_value, change_col, markers = {}, line_styles = {}, colors = {}, y_limit = True):
    set_font()
    # Generate default colors dynamically based on unique labels in 'v'
    unique_labels = df[graph_lines].unique()
    default_colors = {label: plt.cm.tab10(i) for i, label in enumerate(unique_labels)}
    df = df[(df[x] > 0)]
    for d in df[detail].unique():
        df_reduced = df[df[detail] == d]
        df_reduced = df_reduced.sort_values(by=x)
        #fillna values with 0
        df_reduced[y].fillna(0, inplace=True)
        #order by linegraph
        df_reduced = df_reduced.sort_values(by=[graph_lines, x])
        plt.clf()
        graph_values = df_reduced[graphs_value].unique()
        nrows = 2 if len(graph_values) > 2 else 1
        ncols = len(graph_values) if len(graph_values) > 1 else 2
        fig, axes = plt.subplots(nrows=nrows, ncols=ncols, figsize=(10 * ncols, 6 * nrows))
        #fig.suptitle(d)
        handles = []
        labels = []
        seen_labels = {}
        for col in range(ncols if len(graph_values) > 1 else nrows):
            title = graph_values[col]
            df_graph = df_reduced[df_reduced[graphs_value] == title]
            for v in df_graph[graph_lines].unique():
                subset = df_graph[df_graph[graph_lines] == v]
                for row in range(nrows if len(graph_values) > 1 else ncols):
                    ax = axes[row, col] if len(graph_values) > 1 else axes[row]
                    ax.set_xlabel(x_label)
                    if row == 0:
                        #ax.set_title(title)
                        line, = ax.plot(subset[x], subset[y], marker = markers.get(v, 'o'), linestyle= line_styles.get(v, '-'), label=v, color = colors.get(v, default_colors[v]))
                        ax.set_ylabel(y_label)
                        if y_limit:
                            ax.set_ylim(0, 1.1)  # Set y-axis limits to 0-1
                    else:
                        change_points = subset[subset[change_col].diff() == 1]
                        #plot in the x the time and in the y the value of v. Plot a line of subset[change_col] where there is a x if the value of change_col is 1
                        ax.plot(subset[x], subset[graph_lines], linestyle= line_styles.get(v, '-'), label=v, color = colors.get(v, default_colors[v])) # marker = markers.get(v, 'o'),
                        #plot the change points
                        ax.scatter(change_points[x], change_points[graph_lines], color=colors.get(v, default_colors[v]), edgecolor='black', zorder=5, s=100)
                    ax.set_xticks([x for x in range(1, subset[x].max() + 1, 1)])
                # Collect handles and labels for legend, avoiding duplicates
                if v not in seen_labels:
                    handles.append(line)
                    labels.append(v)
                    seen_labels[v] = 1
        # Adjust layout and display the plot
        fig.legend(handles, labels, bbox_to_anchor=(0.3, .9), loc=3, ncol=len(handles), borderaxespad=0.)

        plt.tight_layout()
        fig.subplots_adjust(top=0.89)

        plt.savefig(f"{base_path}/graphs/{d}_{detail}_{x_label}_{y_label}_{graph_lines}_{graphs_value}.png")
        plt.close()