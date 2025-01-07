import polars as pl

input_path = '../data/venture_funding_deals.parquet'
df = pl.read_parquet(
    input_path,
    columns = ['Company', 'Amount', 'Valuation', 'Industry'],
    row_index_name = 'row_cnt'
)
print(pl.read_parquet_schema(input_path))
print(df.head())

df.write_parquet('output.parquet', compression='lz4', compression_level=10)
lf = pl.scan_parquet(input_path)
lf.sink_parquet('lazy_output.parquet', maintain_order=False)
