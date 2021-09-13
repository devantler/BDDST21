from hdfs import InsecureClient
from collections import Counter
import pyarrow.parquet as pq
import pandas as pd
import pyarrow as pa

client = InsecureClient('http://namenode:9870', user='root')

# Make wordcount reachable outside of the with-statement
wordcount = None

with client.read('/alice.txt', encoding='utf-8') as reader:
    wordcount = Counter(reader.read().split()).most_common(10)


# To-Do: Save the wordcount in a Parquet file and read it again!
df = pd.DataFrame(wordcount)
table = pa.Table.from_pandas(df)
pq.write_table(table, 'example.parquet')
table2 = pq.read_table('example.parquet')
print(table2.to_pandas())
