import pandas as pd
import numpy as np

# Define the indexes
indexes = [
    "$\{\emph{``Area''}, \emph{``Task''}\}$",
    "$\{\emph{``Region''},\emph{``Task''}\}$",
    "$\{\emph{``Area''},\emph{``Region''}\}$",
    "$\{\emph{``Goal''},\emph{``Task''}\}$",
]

# Create a dictionary with all combinations and their corresponding values
value_dict = {
    "$\{\emph{``Area''}, \emph{``Task''}\}$": {
        "$\{\emph{``Goal''},\emph{``Task''}\}$": "$0.8 \cdot 0.78 + \\newline  0.2 \cdot \\frac{50}{500} = 0.64$",
        "$\{\emph{``Area''},\emph{``Region''}\}$": "$0.8 \cdot 0.6 + \\newline  0.2 \cdot \\frac{100}{500} = 0.52$",
        "$\{\emph{``Region''},\emph{``Task''}\}$": "$0.8 \cdot 1 + \\newline  0.2 \cdot \\frac{50}{500} = 0.82$"
    },
    "$\{\emph{``Goal''},\emph{``Task''}\}$": {
        "$\{\emph{``Area''},\emph{``Region''}\}$": "$0.8 \cdot 0.38 + \\newline  0.2 \cdot \\frac{50}{100} = 0.40$",
        "$\{\emph{``Region''},\emph{``Task''}\}$": "$0.8 \cdot 0.56 + \\newline  0.2 \cdot \\frac{50}{50} = 0.65$"
    },
    "$\{\emph{``Area''},\emph{``Region''}\}$": {
        "$\{\emph{``Region''},\emph{``Task''}\}$": "$0.8 \cdot 0.6 + \\newline  0.2 \cdot \\frac{50}{100} = 0.58$"
    },
}

# Initialize an empty DataFrame with the specified indexes and columns
df = pd.DataFrame(index=indexes, columns=indexes)

def get_value(row, col):
    try:
        return value_dict[row][col]
    except KeyError:
        return value_dict[col][row]

# Fill the DataFrame
for row in indexes:
    for col in indexes:
        if row == col:
            df.at[row, col] = "--"
        else:
            # For simplicity, using a tuple of tuples to search in the dictionary
            df.at[row, col] = get_value(row, col)

# Display the DataFrame
print(df.to_latex(escape=False))
