import subprocess
import os
from common import base_dir

for script in ["6.3Naive.py", "6.4.1_stats.py", "6.4_frequency_stats.py", "6.4.2_stats.py", "6.4.3_stats.py", "6.5_stats.py"]:
    s = f"{base_dir}/{script}"
    print(f"Running {s}")
    subprocess.run(["python", s])