import matplotlib.pyplot as plt
import numpy as np
from matplotlib.colors import LinearSegmentedColormap
from matplotlib.colorbar import ColorbarBase

def plot_dynamic_bar_charts(time_points, params):
    # Set global use of TeX fonts
    plt.rcParams.update({
        "text.usetex": True,
        "font.family": "serif",  # Change this as per your preference
    })

    font_size = 20
    font_title = 25
    # Number of rows
    num_rows = len(params)

    # Create a figure with the specified number of rows
    fig, axes = plt.subplots(num_rows, 1, figsize=(25, 16), sharex=True)

    # Define the blue gradient colormap
    cmap = LinearSegmentedColormap.from_list("blue_gradient",
                                                 ["#E0F3FF", "#CCE5FF", "#B8D7FF", "#A3C9FF", "#8FBBFF", "#7AADFF", "#669FFF", "#528FFF", "#3E7FFF", "#2A6FFF", "#155FFF", "#004FFF"])

    max_bar_size = {}
    total_bar_width = 0.95  # Adjust total width of bars
    space_between_bars = 0.005  # Adjust space between bars
    space_between_bars_of_bars = 0.2  # Adjust space between bars of bars
    for i, (ax, row_params) in enumerate(zip(axes, params.values())):
        num_bars_list = row_params['num_bars']
        for t_idx, num_bars in enumerate(num_bars_list):
            bar_width = ((total_bar_width - space_between_bars_of_bars) / num_bars) - space_between_bars
            total_bar_size = (num_bars-1) * (bar_width + space_between_bars) + bar_width + space_between_bars_of_bars
            if t_idx not in max_bar_size:
                max_bar_size[t_idx] = total_bar_size
            else:
                max_bar_size[t_idx] = max(max_bar_size[t_idx], total_bar_size)

    for i, (ax, row_params) in enumerate(zip(axes, params.values())):
        # Extract the parameters for this row
        num_bars_list = row_params['num_bars']
        gradient_levels = row_params['gradient_levels'] * 100
        bar_names = row_params['bar_names']
        group_descriptions = row_params['group_descriptions']
        heights = row_params['heights']  # Extract heights from params
        # Calculate total height for each time point in this row
        for t_idx, num_bars in enumerate(num_bars_list):
             # Plot bars for each time point with varying number of bars
            bar_width = (max_bar_size[t_idx] - (num_bars - 1) * space_between_bars - space_between_bars_of_bars) / num_bars
            color = cmap(gradient_levels[t_idx])  # Get color from gradient based on level
            group_start = sum(max_bar_size.get(k, 0) for k in range(t_idx))

            for j in range(num_bars):
                value = heights[t_idx][j]
                bar_position = group_start + j * (bar_width + space_between_bars)  # Calculate bar position
                ax.bar(bar_position, value, width=bar_width, color=color)
                # Adding vertical text
                ax.text(bar_position, 0, bar_names[t_idx][j], fontsize=font_size, color='black', ha='right', va='top', rotation=45)
            # Add group description above the total height
            ax.text(group_start - space_between_bars_of_bars + (max_bar_size[t_idx]) / 2, 108, group_descriptions[t_idx], ha='left', va='bottom', fontsize=font_size, color='black', weight='bold')
            if t_idx < len(num_bars_list) - 1:
                # Add vertical dashed line to separate time points
                print("Adding line")
                #TODO for add lines ax.axvline(group_start + max_bar_size[t_idx] - space_between_bars_of_bars, color='black', linestyle='--')

        #set the y-axis limit to the maximum height
        ax.set_ylim(0,120)
        ax.set_ylabel('Avg(Battery level)', fontsize=font_size)
        ax.set_xticks([i for i in time_points])
        ax.set_yticks([i for i in range(0, 120, 20)])
        ax.tick_params(axis='x', which=u'both',length=0, labelsize=font_size)
        ax.tick_params(axis='y', labelsize=font_size)
        ax.set_xticklabels([])#[f"{i+1}" for i in time_points])
        #ax.set_xlabel('Time', fontsize=font_title)
        #show the grid ax.grid(True)
        # Set the title for each row
        ax.set_title(row_params['title'], fontsize=font_title)
        # Add an arrow to the left with the text 'Time' on the last subplot
        if i == num_rows - 1:
            ax.annotate('', xy=(0, -0.4), xycoords='axes fraction', xytext=(1, -0.4),
                        arrowprops=dict(arrowstyle="<|-, head_length=1, head_width=1", color='black', lw = .8))
            ax.annotate('Time', xy=(0, -0.5), fontsize = font_size, xycoords='axes fraction', xytext=(.5, -0.5), ha='center', va='center')
    # Plot colorbar legend
    colorbar_ax = fig.add_axes([0.93, 0.2, 0.01, 0.6])  # [left, bottom, width, height]

    norm = plt.Normalize(0, 100)
    cb = ColorbarBase(colorbar_ax, cmap=cmap, norm=norm, orientation='vertical')
    cb.set_label('Support', rotation=270, labelpad=20, fontsize=font_size)
    cb.ax.yaxis.set_ticks_position('right')  # Place ticks on the left side
    cb.ax.tick_params(labelsize=font_size)

    plt.subplots_adjust(hspace=0.5)
    # Adjust layout
    #plt.tight_layout()
    plt.savefig('debug/analysis/4.1_example.png', bbox_inches='tight')
# Define the time points
time_length = 5
time_points = np.arange(0, time_length, 1)

# Define the parameters for each row, including 'heights'
params = {
    'row_1': {
        'title': 'Priority to continuity',
        'num_bars': [5, 3, 3, 4, 6],
        'gradient_levels': [0.99, 0.8, 0.3, 0.02, 0.005],
        'bar_names': [
            ['BayArea', 'SiliconV', 'Tasmania', 'Catalonia', 'Galicia'],
            ['BayArea', 'SiliconV', 'Tasmania'],
            ['BayArea', 'SiliconV', 'Tasmania'],
            ['BayArea', 'SiliconV', 'Tasmania', 'Galicia'],
            ['BayArea', 'SiliconV', 'Tasmania', 'Catalonia', 'Galicia', 'London'],
        ],
        'group_descriptions': ['Area'] * time_length,
        'heights': [
            [90, 40, 70, 20, 50],
            [85, 55, 60],
            [80, 60, 55],
            [85, 50, 57, 60],
            [87, 55, 59, 15, 55, 8],
        ]
    },
    'row_2': {
        'title': 'Priority to representativeness',
        'num_bars': [5, 5, 4, 5, 2],
        'gradient_levels': [0.99, 0.95, 0.99, 0.89, 0.95],
        'bar_names': [
            ['BayArea', 'SiliconV', 'Tasmania', 'Catalonia', 'Galicia'],
            ['Weeding', 'Crop monitoring', 'Weed counting', 'Alerting', 'Maintenance'],
            ['ABot', 'Tech', 'RovX', 'Spark'],
            ['GreenA', 'Sunny', 'MapleG', 'OakC', 'PineR'],
            ['2023', '2024']
        ],
        'group_descriptions': [
            'Area', 'Task', 'Robot', 'Farm', 'Activity Year'
        ],
        'heights': [
            [90, 40, 70, 20, 50],
            [50, 90, 40, 20, 50],
            [60, 80, 30, 70],
            [30, 20, 50, 40, 60],
            [95, 90]
        ]
    },
    'row_3': {
        'title': 'Trade-off between continuity and representativeness',
        'num_bars': [5, 3, 2, 3, 4],
        'gradient_levels': [0.99, 0.8, 0.75, 0.85, 0.9],
        'bar_names': [
            ['BayArea', 'SiliconV', 'Tasmania', 'Catalonia', 'Galicia'],
            ['BayArea', 'SiliconV', 'Tasmania'],
            ['USA', 'ES'],
            ['USA', 'ES', 'AU'],
            ['USA', 'ES', 'AU', 'UK'],
        ],
        'group_descriptions': ['Area', 'Area', 'Country', 'Country', 'Country'],
        'heights': [
            [90, 40, 70, 20, 50],
            [85, 55, 60],
            [67, 50],
            [70, 45, 60],
            [75, 55, 60, 10],
        ]
    },
}

# Generate the bar charts
plot_dynamic_bar_charts(time_points, params)
