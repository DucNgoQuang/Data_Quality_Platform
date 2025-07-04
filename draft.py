import pandas as pd 
df = pd.read_parquet('/home/duc/Desktop/VDT_Miniproject/dqops/volume/.data/error_samples/c=bbbbbbbb/t=files.dish_delta/m=2025-05-01/errorsamples.0.parquet')
print(df.to_numpy()[0])