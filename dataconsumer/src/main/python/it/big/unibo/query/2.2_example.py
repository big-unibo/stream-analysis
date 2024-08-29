import pandas as pd
import numpy as np

# Dictionaries for the given data
single_attributes = {
    "Area": 100,
    "Country": 3,
    "Farm": 1000,
    "Goal": 10,
    "Region": 5,
    "Task": 10,
}

pair_attributes = {
    ("Area", "Region"): 100,
    ("Area", "Task"): 500,
    ("Region", "Task"): 50,
    ("Area", "Farm"): 1000,
    ("Farm", "Task"): 9000,
    ("Area", "Country"): 111,
    ("Region", "Country"): 18,
    ("Task", "Country"): 300,
    ("Farm", "Country"): 1000,
    ("Farm", "Region"): 1000,
    ("Region", "Goal"): 85,
    ("Task", "Goal"): 50,
    ("Farm", "Goal"): 1540,
    ("Area", "Goal"): 180,
    ("Country", "Goal"): 80,
}

# Create DataFrame for single attributes
single_df = pd.DataFrame(list(single_attributes.items()), columns=["$a$", "$card(R_{ex}, a)$"])
single_df["$a$"] = single_df["$a$"].apply(lambda x: f"``{x}''")
single_df["$card(R_{ex}, a)$"] = single_df["$card(R_{ex}, a)$"].apply(lambda x: f"${x}$")
print(single_df.to_latex(index=False, escape=False))

# Create DataFrame for pair attributes
pair_df = pd.DataFrame([{"A": f"{pair[0]}, {pair[1]}", "$card(R_{ex}, A)$": cardinality}
                        for pair, cardinality in pair_attributes.items()])


attributes = list(single_attributes.keys())
pair_matrix = pd.DataFrame(index=attributes, columns=attributes, data=np.nan)

# Fill the matrix with provided pair values
for (attr1, attr2), value in pair_attributes.items():
    pair_matrix.at[attr1, attr2] = value
    pair_matrix.at[attr2, attr1] = value

for attr in attributes:
    pair_matrix.at[attr, attr] = single_attributes[attr]

pair_matrix = pair_matrix.fillna(0).astype(int)
# Apply formatting for LaTeX
pair_matrix_latex = pair_matrix.map(lambda x: f"${x}$")
pair_matrix_latex.index = [f"``{attr}''" for attr in pair_matrix_latex.index]
pair_matrix_latex.columns = [f"``{attr}''" for attr in pair_matrix_latex.columns]

print(pair_matrix_latex.to_latex(escape=False))

# Create fds_matrix
fds_matrix = pd.DataFrame(index=attributes, columns=attributes, data=np.nan)
# Fill the new matrix with the maximum values
for attr1 in attributes:
    for attr2 in attributes:
        fds_matrix.at[attr1, attr2] = max(single_attributes[attr1] / pair_matrix.at[attr1, attr2], single_attributes[attr2] / pair_matrix.at[attr1, attr2])

fds_matrix_latex = fds_matrix.map(lambda x: f"${x:.2f}$")
fds_matrix_latex.index = [f"``{attr}''" for attr in fds_matrix_latex.index]
fds_matrix_latex.columns = [f"``{attr}''" for attr in fds_matrix_latex.columns]
print(fds_matrix_latex.to_latex(escape=False))
