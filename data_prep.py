
import pandas as pd

def rows_to_columns(df):
  df = df.drop(columns=['무역수지'])

  # Reshape the dataframe
  df_melted = df.melt(id_vars=['기간', '품목명'], value_vars=['수출 중량', '수출 금액', '수입 중량', '수입 금액'])
  df_melted['variable'] = df_melted['품목명'] + '_' + df_melted['variable']
  df_pivot = df_melted.pivot(index='기간', columns='variable', values='value')
  df_pivot = df_pivot.fillna(0)
  df_reset = df_pivot.reset_index()
  return df_reset

def extract_month_day(df, timestamp_col):

  # Convert the string timestamp to datetime format
  df[timestamp_col] = pd.to_datetime(df[timestamp_col])

  # Extract month and day and add them as new columns
  df['month'] = df[timestamp_col].dt.month
  df['day'] = df[timestamp_col].dt.day

  return df

def merge_on_month_year(df_A, df_B, timestamp_col='timestamp', month_year_col='기간'):

  # Extract month and year from df_A's timestamp
  df_A['month_year'] = df_A[timestamp_col].dt.strftime('%Y-%m')

  # Ensure df_B's month_year_col is of type string
  df_B['month_year'] = df_B[month_year_col].astype(str)

  # Merge the dataframes on month-year
  merged_df = pd.merge(df_A, df_B, left_on='month_year', right_on=month_year_col, how='left')

  # Drop the 'month_year' column we added to df_A for merging
  merged_df.drop(columns='month_year', inplace=True)

  return merged_df

def map_to_timestamp(df_multiindex):
    # Melt the multi-index DataFrame to long format
    df_long = df_multiindex.stack(level=[1, 2, 3]).reset_index()
    df_long.columns = ['timestamp', 'item', 'corporation', 'location', 'value']

    # Ensure that the time_stamp column is of type string
    df_long['timestamp'] = df_long['timestamp'].astype(str)

    # Create the ID column for merging
    df_long['ID'] = df_long['item'] + '_' + df_long['corporation'] + '_' + df_long['location'] + '_' + df_long['timestamp'].str.replace('-', '')

    # Extract unique combinations of item, corporation, and location from the long format DataFrame
    combinations = df_long[['item', 'corporation', 'location']].drop_duplicates()

    # Create a date range from the unique time_stamp values
    date_range = df_multiindex.index.unique()

    # Create a DataFrame with repeated combinations for each date in the date range
    df = pd.concat([combinations] * len(date_range), ignore_index=True)
    df['timestamp'] = date_range.repeat(len(combinations))

    # Ensure that the time_stamp column in the new DataFrame is of type string
    df['timestamp'] = df['timestamp'].astype(str)

    # Create the ID column for the new DataFrame
    df['ID'] = df['item'] + '_' + df['corporation'] + '_' + df['location'] + '_' + df['timestamp'].str.replace('-', '')

    # Merge the new DataFrame with the long format DataFrame on the ID column to map the values
    result = pd.merge(df, df_long, on=['ID', 'item', 'corporation', 'location', 'timestamp'], how='left')

    # Drop the ID column and reorder columns
    result = result.drop('ID', axis=1)
    result = result[['timestamp', 'item', 'corporation', 'location', 'value']]

    return result

def map_timestamp_and_merge(df_A, df_B, columns_to_match):
    # Extract day from the 'timestamp' column in df_A and convert to format similar to df_B (0 to 27)
    df_A['timestamp'] = df_A['timestamp'].str[-2:].astype(int) - 4

    # Debugging print
    print("Unique values in df_A after conversion:", df_A['timestamp'].unique())

    # Convert the timestamp column of df_B from string to integer
    df_B['timestamp'] = df_B['timestamp'].astype(int)

    # Debugging print
    print("Unique values in df_B:", df_B['timestamp'].unique())

    # Merge the dataframes based on the timestamp
    merged_df = pd.merge(df_A, df_B, on=columns_to_match, how='outer')

    # Create a mapping dictionary
    date_range = pd.date_range(start="2023-03-04", end="2023-03-31")
    mapping_dict = {i: date.strftime('%Y-%m-%d') for i, date in enumerate(date_range)}

    # Map the values in merged_df using the dictionary
    merged_df['timestamp'] = merged_df['timestamp'].map(mapping_dict)

    # Convert the date strings to datetime objects
    merged_df['timestamp'] = pd.to_datetime(merged_df['timestamp'])

    return merged_df

def add_day_columns_oh(df: pd.DataFrame, datetime_col: str) -> pd.DataFrame:

    days = df[datetime_col].dt.dayofweek
    for i in range(7):
        df[f'day_of_week_{i}'] = days == i
    return df

def add_day_column(df: pd.DataFrame, datetime_col: str, new_col: str) -> pd.DataFrame:
    df[new_col] = df[datetime_col].dt.dayofweek
    return df

def convert_to_single_named_columns(df: pd.DataFrame, separator: str = '_') -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.map(separator.join)
    return df

def get_zero_timestamps(df: pd.DataFrame, column: str, year: int = None) -> pd.Series:

    if year:
        df = df[df['time_stamp___'].dt.year == year]

    return df.loc[df[column] == 0, 'time_stamp___']
