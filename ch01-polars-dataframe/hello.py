import polars as pl

def main():
    df = pl.DataFrame({
        'nums': [1, 2, 3, 4, 5],
        'letters': ['a', 'b', 'c', 'd', 'e'],
    })
    print(df.flags)


if __name__ == "__main__":
    main()
