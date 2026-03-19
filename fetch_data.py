import requests
import pandas as pd

URL = "https://data.ny.gov/resource/9s3h-dpkz.json"
LIMIT = 1000


def fetch_all_data() -> pd.DataFrame:
    all_rows = []
    offset = 0

    while True:
        params = {"$limit": LIMIT, "$offset": offset}
        try:
            response = requests.get(URL, params=params)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"Error fetching data: {e}")
            break

        data = response.json()

        if not data:
            break
        
        # filter based on state==NY and description==Wholesale Liquor
        filtered = [
            row for row in data
            if row.get('state') == 'NY' and row.get('description') == 'Wholesale Liquor'
        ]
        all_rows.extend(filtered)

        print(f"Fetched {offset+LIMIT} so far, filtered {(offset+LIMIT) - len(all_rows)} out...")

        if len(data) < LIMIT:
            break

        offset += LIMIT

    df = pd.DataFrame(all_rows)
    print(f"Done. Total rows: {len(df)}")
    return df


if __name__ == "__main__":
    df = fetch_all_data()
    print(df.head())
