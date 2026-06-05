import os
import pandas as pd     

pos = pd.read_parquet("../../data/processed/silver/position")
sta = pd.read_parquet("../../data/processed/silver/static")
    ```