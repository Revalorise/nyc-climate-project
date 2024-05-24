import requests
import pandas as pd


def get_data(url):
    df = pd.read_csv(url)
    return df


if __name__ == "__main__":
    url = "https://data.cityofnewyork.us/resource/hmdk-eidg.csv"
    df = get_data(url)
    df.to_csv("../data/temperature.csv", index=False)
