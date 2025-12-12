import requests
import pandas as pd 
import os

# For fetching data from CDC YRBSS API endpoint
Base_url = "https://data.cdc.gov/resource/vba9-s8jp.json"
OUTPUT = "data/clean_youth_obesity.csv"


def fetch_obesity_data(limit=10000):
    """
    Fetch teen obesity data (grades 9–12) from CDC YRBSS.
    Filter Condition = Obesity prevalence.
    """
    params = {
        "$limit": limit,
        "$where": "class = 'Obesity / Weight Status' AND yearstart BETWEEN 2003 AND 2023"
    }

    print("Pulling data from CDC API...")
    response = requests.get(Base_url, params=params)

    if response.status_code != 200:
        raise requests.exceptions.HTTPError(f"Failed to pull data: {response.status_code}")

    data = response.json()
    print(f"Downloaded {len(data)} rows")
    df = pd.DataFrame(data)
    return df

def save_data(df):
    
    
    print("in save")
    keep_cols = [
        "yearend", "locationabbr", "locationdesc",
        "sex", "stratification1", "grade",
        "question", "data_value"
    ]
    df = df[keep_cols]

    # Removing rows with no values
    # df = df.dropna(subset=["data_value", "sex"])
    df["yearend"].astype(int)
    print(df.shape)
    df.to_csv(OUTPUT, index=False)

if __name__ == "__main__":
    df = fetch_obesity_data()
    save_data(df)




