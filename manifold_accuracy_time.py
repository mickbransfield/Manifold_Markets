# Adapted from code created by ChatGPT Jan 2024


# Modules
import polars as pl
import glob
import re
from datetime import datetime, timedelta
import pathlib


# Functions
def extract_datetime_from_filename(filename):
    match = re.search(r'\d{4}-\d{2}-\d{2} \d{2}_\d{2}_\d{2}', filename)
    if match:
        date_str = match.group(0).replace('_', ' ')
        return date_str
    return None

def safe_unix_to_datetime(unix_time):
    try:
        unix_time = int(float(unix_time) / 1000)
        if 0 <= unix_time <= 32503680000:  # Upper limit is 01-01-3000 00:00:00
            return datetime.fromtimestamp(unix_time)
    except (ValueError, TypeError, OverflowError):
        return None
    return None

def convert_string_to_boolean(series):
    return series.map_elements(lambda x: x.lower() in ['true', '1'] if x is not None else None)

# Function to convert 'YES'/'NO' to 1/0
def yes_no_to_numeric(value):
    return 1 if value == 'YES' else 0


# Variables
folder_path = r'C:/Users/Local/Directory/Market_Data/'  # add folder path here

csv_files = glob.glob(folder_path + '*.csv')

required_columns = ["id", "createdTime", "closeTime", "question", "slug", "url", "outcomeType", "volume", "volume24Hours", "isResolved", "uniqueBettorCount", "pool", "probability", "p", "value", "resolution"]

numeric_columns = ["volume", "volume24Hours", "probability", "p", "value", "uniqueBettorCount"]




## READ IN DATA ##

dataframes = []

for file in csv_files:
    df = pl.read_csv(file)

    # Extract filename and datetime
    filename = file.split('/')[-1]
    datetime_value = extract_datetime_from_filename(filename)

    # Add 'Filename' and 'DateTime' columns
    df = df.with_columns([pl.lit(filename).alias('Filename'),
                          pl.lit(datetime_value).alias('DateTime')])

    # Convert all columns to string
    df = df.with_columns([pl.col(column).cast(pl.Utf8) for column in df.columns])

    # Add missing columns as strings
    for column in required_columns:
        if column not in df.columns:
            df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias(column))

    # Reorder columns to match the required_columns order
    df = df.select(['Filename', 'DateTime'] + required_columns)

    dataframes.append(df)

# Concatenate all DataFrames
combined_df = pl.concat(dataframes)

# Cast numeric columns to their appropriate data types
for column in numeric_columns:
    combined_df = combined_df.with_columns(combined_df[column].cast(pl.Float64, strict=False).alias(column))

# Convert 'isResolved' to Boolean
combined_df = combined_df.with_columns(convert_string_to_boolean(combined_df["isResolved"]).alias("isResolved"))

# Cast date columns to datetime
combined_df = combined_df.with_columns([
    pl.col("DateTime").str.strptime(pl.Datetime, "%Y-%m-%d %H %M %S"),
    pl.col("createdTime").map_elements(safe_unix_to_datetime, return_dtype=pl.Datetime),
    pl.col("closeTime").map_elements(safe_unix_to_datetime, return_dtype=pl.Datetime)
])

# Filter for "BINARY" outcomeType
filtered_df = combined_df.filter(pl.col("outcomeType") == "BINARY")



# Create 30 separate dataframes, one for each day
for i in range(30, 0, -1):
    # Copy the 'filtered_df' to create a new dataframe for each day
    new_df = filtered_df
    
    # Rename the new dataframe with a corresponding name
    new_df = new_df.rename({"volume": f"volume_{i}", "probability": f"probability_{i}"}) # "closeTime": f"closeTime_{i}", 
    
    # Assign the new dataframe to a variable with the corresponding name
    exec(f'filt_{i}_df = new_df')



## FILTER TO RESOLVED BINARY (YES/NO) MARKETS ##

# Filter to Resolution of YES or NO
filtered_df = combined_df.filter((pl.col("resolution") == "YES") | (pl.col("resolution") == "NO"))

# Filter for resolved markets
resolved_markets_df = filtered_df.filter(pl.col("isResolved") == True)

# Then, sort by 'DateTime' in descending order
sorted_markets_df = resolved_markets_df.sort("DateTime", descending=True)

# Deduplicate, keeping only the last (most recent) entry for each market identifier
final_df = sorted_markets_df.unique(subset=["id"], keep="last")

# Rename columns to distinguish from other dates/dataframes
final_df = final_df.rename({"resolution": "resolution_final"})



## FILTER TO RESPECTIVE DAYS OUT ##

# Ensure 'closeTime' and 'DateTime' are in datetime format
filt_30_df = filt_30_df.with_columns([
    pl.col("closeTime").cast(pl.Datetime),
    pl.col("DateTime").cast(pl.Datetime)
])

# Extract just the date part from 'closeTime' and 'DateTime'
filt_30_df = filt_30_df.with_columns([
    pl.col("closeTime").dt.date().alias("CloseDate"),
    pl.col("DateTime").dt.date().alias("MarketDate")
])

# Calculate the date 30 days before closing
filt_30_df = filt_30_df.with_columns(
    (pl.col("CloseDate") - pl.duration(days=30)).alias("DaysBeforeClose_30")
)

# Filter for markets 30 days before closing
filt_30_df = filt_30_df.filter(pl.col("MarketDate") == pl.col("DaysBeforeClose_30"))

# Sort by 'closeTime' in descending order
filt_30_df = filt_30_df.sort("closeTime", descending=True)

# Deduplicate, keeping only the last (most recent) entry for each market identifier
filt_30_df = filt_30_df.unique(subset=["id"], keep="last")

# Create list of Market IDs for filt_30_df
id_list = filt_30_df['id'].to_list()

# Loop from 29 to 1 days for the already created dataframes
for i in range(29, 0, -1):
    # Get the already created dataframe, e.g., filt_29_df
    df = globals().get(f'filt_{i}_df')
    
    # Ensure 'closeTime' and 'DateTime' are in datetime format
    df = df.with_columns([
        pl.col("closeTime").cast(pl.Datetime),
        pl.col("DateTime").cast(pl.Datetime)
    ])
    
    # Extract just the date part from 'closeTime' and 'DateTime'
    df = df.with_columns([
        pl.col("closeTime").dt.date().alias("CloseDate"),
        pl.col("DateTime").dt.date().alias("MarketDate")
    ])
    
    # Calculate the date i days before closing
    df = df.with_columns(
        (pl.col("CloseDate") - pl.duration(days=i)).alias(f"DaysBeforeClose_{i}")
    )
    
    # Filter for markets i days before closing with the same Market IDs as filt_30_df
    df = df.filter(pl.col("MarketDate") == pl.col(f"DaysBeforeClose_{i}")).filter(pl.col("id").is_in(id_list))
    
    # Sort by 'closeTime' in descending order
    df = df.sort("closeTime", descending=True)
    
    # Deduplicate, keeping only the last (most recent) entry for each market identifier
    df = df.unique(subset=["id"], keep="last")
    
    # Update the existing dataframe with the changes
    globals()[f'filt_{i}_df'] = df


# Loop from 30 to 1 days for the already created dataframes
for i in range(30, 0, -1):
    # Get the already created dataframe, e.g., filt_29_df
    df = globals().get(f'filt_{i}_df')
    
    # Rename the "closeTime" column to "closeTime_i"
    df = df.rename({"closeTime": f"closeTime_{i}"})
    
    # Filter down to columns of interest
    df = df.select(["id", f"DaysBeforeClose_{i}", f"closeTime_{i}", f"volume_{i}", f"probability_{i}"])
    
    # Update the existing dataframe with the renamed closeTime and filtered columns
    globals()[f'filt_{i}_df'] = df



## MERGE DATAFRAMES TOGETHER ##

# Inner join filt_30_df with final_df
merged_df = final_df.join(filt_30_df, on="id", how="inner")

# Define a list of remaining dataframes
remaining_dataframes = [globals()[f'filt_{i}_df'] for i in range(29, 0, -1)]

# Left join the remaining dataframes in order
for i, df in enumerate(remaining_dataframes):
    if i == 0:
        # For the first dataframe, perform a left join with merged_df
        merged_df = merged_df.join(df, on="id", how="left")
    else:
        # For subsequent dataframes, perform a left join with merged_df
        merged_df = merged_df.join(df, on="id", how="left", suffix=f"_{i}")

# Filter out rows where closeTime 30 days out doesn't equal closeTime 1 day out
merged_df = merged_df.filter(merged_df["closeTime_30"] == merged_df["closeTime_1"])



## FORECASTING ERROR ##

# Apply yes_no_to_numeric function to create a new column
merged_df = merged_df.with_columns(
    pl.col('resolution_final').map_elements(yes_no_to_numeric).alias('Resolution_numeric')
)

# Error for all days out
for i in range(30, 0, -1):
    # Create the 'error_{i}' column by subtracting 'probability_{i}' from 'Resolution_numeric'
    merged_df = merged_df.with_columns(
        (merged_df['Resolution_numeric'] - merged_df[f'probability_{i}']).alias(f'error_{i}')
    )

# Squared error for all days out (Brier score)
for i in range(30, 0, -1):
    # Create the 'squared_error_{i}' column by squaring 'error_{i}'
    merged_df = merged_df.with_columns(
        (merged_df[f'error_{i}'] ** 2).alias(f'squared_error_{i}')
    )

# Replace 0 with 0.0000001 and 1 with 0.9999999
merged_df = merged_df.with_columns(
    pl.when(merged_df['Resolution_numeric'] == 0).then(0.00000000000001)
     .when(merged_df['Resolution_numeric'] == 1).then(0.99999999999999)
     .otherwise(merged_df['Resolution_numeric'])
     .alias('Resolution_numeric_clipped')
)

# Log Loss for all days out
for i in range(30, 0, -1):
    # Create the 'log_loss_{i}' column by calculating log loss
    log_loss_expression = -(
        merged_df['Resolution_numeric_clipped'] * pl.col(f'probability_{i}').log() +
        (1 - merged_df['Resolution_numeric_clipped']) * (1 - pl.col(f'probability_{i}')).log()
    )
    
    merged_df = merged_df.with_columns(
        log_loss_expression.alias(f'log_loss_{i}')
    )


# Write out ALL dataframe to a CSV file
path: pathlib.Path = "C:/Users/Local/Directory/manifold_market_data_merged_1_through_30.csv" # add file path here
merged_df.write_csv(path, separator=",")




## FILTER OUT NULL VALUES ##

# List all column names
column_names = merged_df.columns

# Create a filter expression that checks for null values in columns starting with "probability_"
filter_expression = None
for column_name in column_names:
    if column_name.startswith("probability_"):
        if filter_expression is None:
            filter_expression = pl.col(column_name).is_not_null()
        else:
            filter_expression &= pl.col(column_name).is_not_null()

# Filter out rows where any "probability_" column has a null value
merged_df = merged_df.filter(filter_expression)


# Write the no_nulls dataframe to a CSV file
path: pathlib.Path = "C:/Users/Local/Directory/manifold_market_data_merged_1_through_30_no_nulls.csv" # add file path here
merged_df.write_csv(path, separator=",")


print(merged_df.head(10))
print(len(merged_df))



## Pivot Table of error functions ##

# Define the prefixes for column selection
prefixes = ["squared_error_", "log_loss_"]

# Select columns that start with any of the prefixes
selected_columns = [column for column in merged_df.columns if any(column.startswith(prefix) for prefix in prefixes)]

# Create a dictionary to map original column names to their day numbers
day_numbers = {column: int(column.split("_")[-1]) for column in selected_columns if column.split("_")[-1].isdigit()}

# Calculating mean for selected columns
mean_values = merged_df.select([pl.col(column).mean().alias(f"{column}_mean") for column in selected_columns])



# Creating lists for squared error, log loss, and days out
squared_error_data = []
log_loss_data = []
days_out = []

# Iterate over the columns in mean_values and fill the lists
for column in mean_values.columns:
    mean_val = mean_values[column][0]
    original_column_name = column.replace("_mean", "")
    if original_column_name in day_numbers:
        day_number = day_numbers[original_column_name]
        if original_column_name.startswith("squared_error_"):
            squared_error_data.append(mean_val)
            days_out.append(day_number)  # Append day number only once for each unique suffix
        elif original_column_name.startswith("log_loss_"):
            log_loss_data.append(mean_val)

# Check if the lists have the same length
if len(squared_error_data) != len(log_loss_data):
    warnings.warn("The lists squared_error_data and log_loss_data have different lengths. This may lead to data misalignment.")

# Creating the final DataFrame
reshaped_df = pl.DataFrame({
    "Days_out": days_out, 
    "squared_error": squared_error_data, 
    "log_loss": log_loss_data
})

print(reshaped_df)

# Write the pivot table to a CSV file
path: pathlib.Path = "C:/Users/Local/Directory/manifold_market_data_pivot.csv" # Add file path here
reshaped_df.write_csv(path, separator=",")