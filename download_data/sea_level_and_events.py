import json
import requests


def get_data(url):
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException as e:
        print(e)
        return None
    data = json.loads(response.text)
    return data


if __name__ == '__main__':
    url = 'https://data.cityofnewyork.us/resource/38ps-fnsg.json'
    data = get_data(url)

    with open('../data/sea_level.json', 'w') as f:
        json.dump(data, f)
