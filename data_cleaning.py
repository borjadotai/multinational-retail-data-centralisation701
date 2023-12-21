import re
import numpy as np
import pandas as pd
import phonenumbers
from datetime import datetime

class DataCleaning:
    def __init__(self):
        # Initialize if needed
        pass

    @staticmethod
    def clean_user_data(df):
        """
        Cleans user data in a DataFrame.
        :param df: DataFrame containing user data.
        :return: Cleaned DataFrame.
        """
        # Handle NULL values
        df.dropna(inplace=True)  # Dropping rows where any column is NaN, can be adjusted

        # Correcting date errors
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], errors='coerce')
        df['join_date'] = pd.to_datetime(df['join_date'], errors='coerce')

        # Standardize phone numbers
        df['phone_number'] = df['phone_number'].apply(DataCleaning._standardize_phone_number)

        return df

    @staticmethod
    def _standardize_phone_number(phone):
        try:
            parsed_phone = phonenumbers.parse(phone, None)
            return phonenumbers.format_number(parsed_phone, phonenumbers.PhoneNumberFormat.E164)
        except phonenumbers.NumberParseException:
            return None  # or handle as appropriate

    def clean_card_data(self, df):
        # Drop rows with any NULL values
        df = df.dropna()

        # Ensure card_number is a string and remove non-numeric characters
        df['card_number'] = df['card_number'].astype(str).str.replace(r'\D', '', regex=True)

        # Check expiry_date format (MM/YY)
        df['expiry_date'] = df['expiry_date'].apply(self._validate_expiry_date)

        # You can add validation for card_provider as needed

        # Ensure date_payment_confirmed is a valid date
        df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'], errors='coerce')
        df = df.dropna(subset=['date_payment_confirmed'])

        return df

    def _validate_expiry_date(self, date_str):
        # Validate and format expiry_date
        if re.match(r'^\d{2}/\d{2}$', date_str):
            return date_str
        else:
            return pd.NA
        
    def clean_store_data(self, df):
        df_cleaned = df.copy()

        # Replace 'N/A', 'None', and None with NaN
        df_cleaned.replace(['N/A', 'None', None], np.nan, inplace=True)

        # Convert 'index', 'longitude', and 'latitude' to numeric, set errors to 'coerce' to handle invalid values
        numeric_columns = ['index', 'longitude', 'latitude']
        for col in numeric_columns:
            df_cleaned[col] = pd.to_numeric(df_cleaned[col], errors='coerce')

        # Remove rows with NaN in critical columns (like 'address')
        df_cleaned.dropna(subset=['address'], inplace=True)

        # Standardize date format in 'opening_date'
        df_cleaned['opening_date'] = pd.to_datetime(df_cleaned['opening_date'], errors='coerce')

        # Clean and standardize string columns
        text_columns = ['locality', 'store_type', 'country_code', 'continent']
        for col in text_columns:
            df_cleaned.loc[:, col] = df_cleaned[col].str.title().str.strip()

        # Return the cleaned DataFrame
        return df_cleaned
    
    def convert_product_weights(self, products_df):
            # Function to convert weight to kg
            def to_kg(weight):
                if pd.isna(weight):
                    return None

                # Extract the numeric part and the unit
                match = re.match(r'(\d*\.?\d+)([a-zA-Z]+)', weight)
                if not match:
                    return None

                value, unit = match.groups()
                value = float(value)

                # Convert to kg based on the unit
                if unit == 'g':
                    return value / 1000  # Convert grams to kilograms
                elif unit == 'kg':
                    return value  # Already in kilograms
                else:
                    return None  # Unhandled unit

            # Apply the conversion to the weight column
            products_df['weight'] = products_df['weight'].apply(to_kg)

            return products_df

    def clean_products_data(self, df):
        # Create a copy of the DataFrame
        cleaned_df = df.copy()

        # Handling missing values - Example: drop rows where 'product_name' or 'product_price' is missing
        cleaned_df.dropna(subset=['product_name', 'product_price'], inplace=True)

        # Standardize text formats - Example: strip whitespace and convert to title case
        text_columns = ['product_name', 'uuid', 'removed', 'product_code']
        for col in text_columns:
            cleaned_df[col] = cleaned_df[col].str.strip().str.title()

        # Remove duplicate rows based on uuid
        cleaned_df.drop_duplicates(subset=['uuid'], inplace=True)

        # Handle product_price - remove non-numeric characters and convert to float (all prices are in gbp)
        cleaned_df['product_price'] = cleaned_df['product_price'].str.extract('(\d+\.\d+|\d+)').astype(float)

        return cleaned_df
    
    def clean_orders_data(self, orders_df):
        # Create a copy of the DataFrame to avoid modifying the original data
        cleaned_orders_df = orders_df.copy()

        # Remove unnecessary columns
        columns_to_remove = ['first_name', 'last_name', '1']
        cleaned_orders_df.drop(columns=columns_to_remove, errors='ignore', inplace=True)

        return cleaned_orders_df

    def clean_date_data(self, df):
        cleaned_df = df.copy()
        cleaned_df.dropna(inplace=True)
        cleaned_df.drop_duplicates(inplace=True)
        return cleaned_df
