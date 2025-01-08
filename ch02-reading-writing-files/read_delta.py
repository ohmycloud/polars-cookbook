import polars as pl
from polars.io.delta import read_delta

def read_delta_lake():
    input_path = '../data/venture_funding_deals_delta'
    df = pl.read_delta(input_path)
    print(df.head())

    lf = pl.scan_delta(input_path)
    print(lf.head().collect())

    # write a DataFrame to a Delta Lake table
    df.write_delta(
        '../data/output/venture_funding_deals_delta_out',
        mode='overwrite',
        delta_write_options={'partition_by': 'Industry'}
    )

    # read only a partition
    df = pl.read_delta(
        '../data/output/venture_funding_deals_delta_out',
        use_pyarrow=True,
        pyarrow_options={'partitions': [('Industry', '=', 'Accounting')]}
    )
    print(df.head())

if __name__ == '__main__':
    read_delta_lake()
