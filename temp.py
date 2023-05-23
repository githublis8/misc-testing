import pandas as pd
import pyspark.pandas as ps

pd.options.display.max_colwidth = 200
pd.set_option('display.width', 200)
pd.set_option('display.max_rows', None)       #Reset the default number of rows python allows to be displayed so that it won't truncate my printout
pd.set_option('display.max_columns', None)    #Reset the default number of columns Python allows to be displayed so it won't truncate printout

file_path = "abfss://datalakegen2filesystem@datalakestoragenfsk522vz.dfs.core.windows.net/analytics/statefarm/scores_auto_3q2020_new_1/"
file = 'part-00000-a2492a0f-6d5d-4f70-a331-d9426c4dd229-c000.snappy.parquet'

#df = pd.read_parquet(file_path+"part-00000-a2492a0f-6d5d-4f70-a331-d9426c4dd229-c000.snappy.parquet", engine='auto')

df = ps.read_parquet(file_path+"part-00000-a2492a0f-6d5d-4f70-a331-d9426c4dd229-c000.snappy.parquet", engine='auto')

print(' ===== top 20 records of data frame =====\n', df.head(20), '\n\n')
#print(' ===== last 20 records of data frame =====\n', df.tail(20), '\n\n')
print('data frame dimension:', df.shape)            
