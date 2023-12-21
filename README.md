# Multinational Retail Data Centralisation

Project in which we read a bunch of different data sources, process them and load them into a postgres DB.

## How to run it

All functionality is written as methods in each of the corresponding classes:

- Data Extraction
- Data Cleaning
- Database Connection

The different tasks implemented as part of the project are run on `main.py` and so, you can go into that file,
uncomment whichever task you want to run and then just run `python main.py`.

In order to run it though, you'll first need to install dependencies, I reccommend you do so by creating a pip
virtual env and then running `pip install -r requirements.txt` since I added all deps there.
