import polars as pl

column_names = ['invoice_no', 'customer_id', 'gender', 'age', 'category', 'quantity',
                 'price', 'payment_method','invoice_date', 'shopping_mall']

df = pl.read_csv('../data/customer_shopping_data_no_header.csv',
                 has_header=False,
                 new_columns=column_names,
                 try_parse_dates=True,
                 schema_overrides={'age': pl.Int8, 'quantity': pl.Int32})
print(df.head())
