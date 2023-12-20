import tabula
import requests
import pandas as pd
from io import BytesIO
from data_cleaning import DataCleaning
from utils import authenticated_request
from concurrent.futures import ThreadPoolExecutor

class DataExtractor:
    def __init__(self):
        pass

    def retrieve_pdf_data(self, url):
        # Download the PDF from the provided URL
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception("Failed to download the file from the provided URL")

        # Convert the downloaded content into a file-like object
        file_like_object = BytesIO(response.content)

        # Use tabula to read PDF file
        # You might need to adjust options depending on your PDF structure
        dfs = tabula.read_pdf(file_like_object, pages='all', multiple_tables=True)

        # dfs will be a list of DataFrames. You can return them separately
        # or concatenate them into a single DataFrame depending on your needs
        # Here's how you can concatenate them:
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    @staticmethod
    def list_number_of_stores(number_stores_url, api_key):
        res = authenticated_request(number_stores_url, api_key)
        return res["number_stores"]

    @staticmethod
    def retrieve_stores_data(store_details_url_base, api_key, total_stores):
        # List to hold store data
        stores_data = []

        def worker_fun(store_number):
            store_url = f"{store_details_url_base}/{store_number}"
            response = authenticated_request(store_url, api_key)
            if response:
                stores_data.append(response)

        # Could increase threads for faster work
        with ThreadPoolExecutor(max_workers=1) as executor:
            executor.map(worker_fun, [i for i in range(451)])

        # Convert the list of dictionaries to a DataFrame
        return pd.DataFrame(stores_data)
