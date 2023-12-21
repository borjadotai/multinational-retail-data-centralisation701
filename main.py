from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from database_utils import DatabaseConnector

def main(task_number):

    # Constants
    aicore_db_creds = 'db_creds.yaml'
    local_db_creds = 'local_db_creds.yaml'
    api_key = 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'
    s3_products_address = 's3://data-handling-public/products.csv'
    s3_date_events_address = 's3://data-handling-public/date_details.json'
    number_stores_url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
    store_details_url_base = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details'
    card_details_pdf_url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'

    # Database engines
    aicore_rds_db = DatabaseConnector(aicore_db_creds)
    local_postgres_db = DatabaseConnector(local_db_creds)

    # General
    cleaner = DataCleaning()
    extractor = DataExtractor()

    # Tasks implementation

    def task_3():
        # 1. Find users table -> table = 'legacy_users'
        # 2. Read table and turn to dataframe
        users_df = aicore_rds_db.read_rds_table('legacy_users')
        # 3. Clean dataframe
        clean_data = data_cleaner.clean_user_data(users_df)
        # 4. Upload data to local db
        local_postgres_db.upload_to_db(clean_data, 'dim_users')

    def task_4():
        card_details = extractor.retrieve_pdf_data(card_details_pdf_url)
        clean_card_details = cleaner.clean_card_data(card_details)
        local_postgres_db.upload_to_db(clean_card_details, 'dim_card_details')

    def task_5():
        # 1. Use list_number_of_stores to see how many stores
        stores_num = extractor.list_number_of_stores(number_stores_url, api_key)
        # Stores number = 451
        # 2. Retrieve stores data
        stores_data = extractor.retrieve_stores_data(store_details_url_base, api_key, stores_num)
        # 3. Clean the data
        clean_stores_data = cleaner.clean_store_data(stores_data)
        # 4. Upload data
        local_postgres_db.upload_to_db(clean_stores_data, 'dim_store_details')

    def task_6():
        s3_product_details = extractor.extract_from_s3(s3_products_address)
        standarised_product_details = cleaner.convert_product_weights(s3_product_details)
        clean_product_details = cleaner.clean_products_data(standarised_product_details)
        local_postgres_db.upload_to_db(clean_product_details, 'dim_products')

    def task_7():
        # 1. Figure out orders table name = orders_table
        orders = aicore_rds_db.read_rds_table('orders_table')
        clean_orders = cleaner.clean_orders_data(orders)
        local_postgres_db.upload_to_db(clean_orders, 'orders_table')

    def task_8():
        s3_date_events = extractor.extract_from_s3(s3_date_events_address, 'json')
        clean_events = cleaner.clean_date_data(s3_date_events)
        local_postgres_db.upload_to_db(clean_events, 'dim_date_times')

    def default_case():
        print("This is a default case for numbers outside 3-8")

    switch = {
        3: task_3,
        4: task_4,
        5: task_5,
        6: task_6,
        7: task_7,
        8: task_8,
    }

    switch.get(task_number, default_case)()

if __name__ == "__main__":
    try:
        number = int(input("Please pick the task you want to run (3 - 8): "))
        if 3 <= number <= 8:
            print(f"Running task number {number}")
            main(number)
        else:
            print("Error: Number must be between 3 and 8 (inclusive).")
    except ValueError:
        print("Error: Invalid input, please enter a number.")
