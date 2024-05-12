def replace_header_with_top_row(df):
    new_header = df.iloc[0]
    df = df[1:]
    df.columns = new_header
    return df
