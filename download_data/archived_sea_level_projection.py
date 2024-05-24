import requests


def download_url(url, save_path, chunk_size=128):
    try:
        r = requests.get(url, stream=True)
    except requests.exceptions.ConnectionError:
        print('Connection Error')
        return 0
    with open(save_path, 'wb') as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            fd.write(chunk)


if __name__ == '__main__':
    url = 'https://data.cityofnewyork.us/api/views/38ps-fnsg/files/2bfeb9d8-a7ae-4575-80bb-44753f9814f5?download=true&filename=Archived_Projected%20Sea%20Level%20Rise.zip'
    download_url(url, '../data/archived_SL_projection.zip')
