# Multinational Retail Data Centralisation

Project in which we read a bunch of different data sources, process them and load them into a postgres DB.

## Project Structure

All functionality is written as methods in each of the corresponding classes:

- Data Extraction
- Data Cleaning
- Database Connection

Each class contains methods focused on that are that our general `main.py` script can then use by creating an
instance of each class and then calling it's methods.

## How to run it

In order to run the script, you'll first need to install dependencies, I reccommend you do so by creating a pip
virtual env and then running `pip install -r requirements.txt` since I added all deps there.

You can then simply run `python main.py` or `python3 main.py` depending on your setup and it will ask you for the
task you want, from 3 to 8. It will then run the script that executes everything that the task asked.
