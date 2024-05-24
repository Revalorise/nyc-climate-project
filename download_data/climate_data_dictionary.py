import openpyxl


def get_data_dictionary(url):
    wb = openpyxl.load_workbook(url)
    wb.save('../data/climate_data_dictionary.csv', index=False)


if __name__ == '__main__':
    url = 'https://data.cityofnewyork.us/api/views/hmdk-eidg/files/f5006193-e0e0-4a23-be95-0fea0b577a6d?download=true&filename=New%20York%20City%20Climate%20Projections%20Data%20Dictionary.xlsx'
    get_data_dictionary(url)
