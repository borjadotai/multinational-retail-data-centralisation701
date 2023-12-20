import requests

def authenticated_request(url, api_key):
    
    headers = {
        'x-api-key': api_key
    }
    
    # Make a GET request to the API endpoint
    response = requests.get(url, headers=headers)

    # Check if the request was successful
    if response.status_code == 200:
        # Return the number of stores
        return response.json()  # Assuming the response is in JSON format
    else:
        # Handle potential errors (e.g., network issues, invalid endpoint, etc.)
        response.raise_for_status()