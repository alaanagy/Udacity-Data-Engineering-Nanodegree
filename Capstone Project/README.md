# Data Engineering Capstone Project

## Immigration to USA

### Project Summary

We will work with four datasets to complete the project. The main dataset will include data on immigration to the United States, and supplementary datasets will include data on airport codes, U.S. city demographics, and temperature data. 

### Instructions

- Step 1: Scope the Project and Gather Data
- Step 2: Explore and Assess the Data
- Step 3: Define the Data Model
- Step 4: Run ETL to Model the Data
- Step 5: Complete Project Write Up


### Datasets


* **I94 Immigration Data**: This data comes from the US National Tourism and Trade Office and includes the contents of the i94 form on entry to the united states. A data dictionary is included in the workspace.
    * _countries.csv_ : table containing country codes used in the dataset, extracted from the data dictionary
    * _i94portCodes.csv_: table containing city codes used in the dataset, extracted from the data dictionary
  The dataset can be found [here.](https://travel.trade.gov/research/reports/i94/historical/2016.html)

* **World Temperature Data**: This dataset comes from Kaggle and includes the temperatures of various cities in the world fomr 1743 to 2013.
  The dataset can be found [here.](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data)
* **U.S. City Demographic Data**: This data comes from OpenSoft. It contains information about the demographics of all US cities and census-designated places with a population greater or equal to 65,000 and comes from the US Census Bureau's 2015 American Community Survey.
  The dataset can be found [here.](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/)
* **Airport Code Table**: This is a simple table of airport codes and corresponding cities.
  The dataset can be found [here.](https://datahub.io/core/airport-codes#data)


### Accessing the Data
Some of the data is already uploaded to the workspace, which you'll see in the navigation pane within Jupyter Lab. The immigration data and the global temperate data is in an attached disk.

#### Immigration Data
You can access the immigration data in a folder with the following path: ../../data/18-83510-I94-Data-2016/. There's a file for each month of the year. An example file name is i94_apr16_sub.sas7bdat. Each file has a three-letter abbreviation for the month name. So a full file path for June would look like this: ../../data/18-83510-I94-Data-2016/i94_jun16_sub.sas7bdat. Below is what it would look like to import this file into pandas.
**Note: these files are large, so you'll have to think about how to process and aggregate them efficiently.**

![image](https://user-images.githubusercontent.com/49722916/232233718-defe92d1-507c-4837-bdcf-499dc25ed6d9.png)

The most important decision for modeling with this data is thinking about the level of aggregation. Do you want to aggregate by airport by month? Or by city by year? This level of aggregation will influence how you join the data with other datasets. There isn't a right answer, it all depends on what you want your final dataset to look like.

#### Temperature Data

You can access the temperature data in a folder with the following path: ../../data2/. There's just one file in that folder, called GlobalLandTemperaturesByCity.csv. Below is how you would read the file into a pandas dataframe.

![image](https://user-images.githubusercontent.com/49722916/232233861-47897d07-0d44-4751-835c-f0893a7d7131.png)


#### Files provided:

All the project details and analysis will be in the project notebook.
