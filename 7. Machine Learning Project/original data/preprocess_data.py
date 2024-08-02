import pandas as pd

# Lists of state acronyms and state names
states_acronym = ["CA", "DE", "FR", "GB", "IN", "JP", "KR", "MX", "RU", "US"]
states_list = ["Canada", "Germany", "France", "United Kingdom", "India", "Japan",
               "South Korea", "Mexico", "Russia", "United States"]

# List to append the DataFrames
dataframes = []

# For cycle for starting to import the unprocessed data files
for acronym, state in zip(states_acronym, states_list):
    # Create a variable name pattern before importing
    filename_csv = f"data/{acronym}videos.csv"
    filename_json = f"data/{acronym}_category_id.json"

    # Import the csv files as Pandas DataFrames
    df_csv = pd.read_csv(filename_csv, encoding='latin1')
    # Add to every DataFrame (from csv) a column with the name of the current STATE
    df_csv["state"] = state

    # Import the json files as Pandas DataFrames
    df_json = pd.read_json(filename_json)
    # Access to the column "items" --> pandas Series
    df_json = df_json["items"]
    # Normalize the data to flatten the nested structure
    df_json_normalized = pd.json_normalize(df_json)

    # Create a dictionary key=id and value=category
    category_dict = dict(zip(df_json_normalized['id'], df_json_normalized['snippet.title']))

    # Change the type of "category_id"  → need it in str type to be mapped correctly
    df_csv["category_id"] = df_csv['category_id'].astype(str)

    # Mapping "category_id" with category title name
    df_csv["category"] = df_csv["category_id"].map(category_dict)

    # Delete the column "category_id"
    df_csv.drop("category_id", axis=1, inplace=True)

    # Add DataFrame to list
    dataframes.append(df_csv)


# Combine all DataFrames into one
data = pd.concat(dataframes, ignore_index=True)

# Save the new dataset into .csv file  → index=False avoids duplicating the index column
data.to_csv("preprocessed_data/preprocessed_data.csv", index=False)
