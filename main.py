from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from database_utils import DatabaseConnector

def main():

    # Constants
    aicore_db_creds = 'db_creds.yaml'
    local_db_creds = 'local_db_creds.yaml'
    api_key = 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'
    number_stores_url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
    store_details_url_base = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details'
    card_details_pdf_url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'

    # Database engines
    aicore_rds_db = DatabaseConnector(aicore_db_creds)
    local_postgres_db = DatabaseConnector(local_db_creds)

    # General
    cleaner = DataCleaning()
    extractor = DataExtractor()


    def user_data():
        # 1. Find users table -> table = 'legacy_users'
        # 2. Read table and turn to dataframe
        users_df = aicore_rds_db.read_rds_table('legacy_users')
        # 3. Clean dataframe
        clean_data = data_cleaner.clean_user_data(users_df)
        # 4. Upload data to local db
        local_postgres_db.upload_to_db(clean_data, 'dim_users')

    def card_details():
        card_details = extractor.retrieve_pdf_data(card_details_pdf_url)
        clean_card_details = cleaner.clean_card_data(card_details)
        local_postgres_db.upload_to_db(clean_card_details, 'dim_card_details')

    def stores():
        # 1. Use list_number_of_stores to see how many stores
        stores_num = extractor.list_number_of_stores(number_stores_url, api_key)
        # Stores number = 451
        # 2. Retrieve stores data
        stores_data = extractor.retrieve_stores_data(store_details_url_base, api_key, stores_num)
        # 3. Clean the data
        clean_stores_data = cleaner.clean_store_data(stores_data)
        # 4. Upload data
        local_postgres_db.upload_to_db(clean_stores_data, 'dim_store_details')


    # ==== RUN METHODS =====
    user_data()
    card_details()
    stores()

if __name__ == "__main__":
    main()
