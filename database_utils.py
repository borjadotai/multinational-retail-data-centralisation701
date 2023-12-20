import yaml
import pandas as pd
from data_cleaning import DataCleaning
from sqlalchemy import create_engine, inspect

class DatabaseConnector:
    def __init__(self, creds_yaml_path):
        self.engine = self.init_db_engine(creds_yaml_path)

    @staticmethod
    def read_db_creds(file_path):
        """
        Reads database credentials from a YAML file.
        :param file_path: Path to the YAML file containing the credentials.
        :return: Dictionary of database credentials.
        """
        try:
            with open(file_path, 'r') as file:
                creds = yaml.safe_load(file)
                return creds
        except Exception as e:
            print(f"Error reading the YAML file: {e}")
            return None

    @staticmethod
    def init_db_engine(creds_yaml_path):
        """
        Initializes and returns an SQLAlchemy database engine using credentials from a YAML file.
        :param creds_yaml_path: Path to the YAML file containing database credentials.
        :return: An SQLAlchemy engine.
        """
        creds = DatabaseConnector.read_db_creds(creds_yaml_path)
        if creds:
            # Construct the database URL
            db_url = f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
            # Create and return the engine
            engine = create_engine(db_url)
            return engine
        else:
            print("Database credentials could not be read.")
            return None
        
    def list_db_tables(self, creds_yaml_path):
        """
        Lists all tables in the database.
        :param creds_yaml_path: Path to the YAML file containing database credentials.
        :return: List of table names.
        """
        engine = self.init_db_engine(creds_yaml_path)
        if engine:
            inspector = inspect(engine)
            return inspector.get_table_names()
        else:
            print("Unable to create database engine.")
            return []
        
    def read_rds_table(self, table_name):
        """
        Extracts data from a specified table in the RDS database.
        :param table_name: Name of the table to extract data from.
        :return: DataFrame containing the data from the table.
        """
        if self.engine:
            query = f"SELECT * FROM {table_name};"
            return pd.read_sql(query, self.engine)
        else:
            print("Database engine is not initialized.")
            return None
        
    def upload_to_db(self, df, table_name, if_exists='replace', index=False):
        """
        Uploads a DataFrame to the specified table in the database.
        :param df: DataFrame to be uploaded.
        :param table_name: Name of the table where the DataFrame will be uploaded.
        :param if_exists: How to behave if the table already exists. Default is 'replace'.
                          Options include: 'fail', 'replace', 'append'.
        :param index: Whether to write DataFrame index as a column. Default is False.
        """
        try:
            df.to_sql(name=table_name, con=self.engine, if_exists=if_exists, index=index)
            print(f"Data uploaded successfully to {table_name}.")
        except Exception as e:
            print(f"Error uploading data to the database: {e}")
