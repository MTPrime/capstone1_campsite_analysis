# Capstone 1 - Campsite Analysis or: How I Learned to Stop Worrying and Love Structured Data
Analyzing the recreation.gov data to find what traits make desirable campsites.


## Table of Contents

## Description
Recreation.Gov is the US Government's web portal for all its reservation needs. It covers tours, facility rentals, campsites and others.
## Case Study Goal
My goal for this case study was to get the unstructured attribute data for each campsite from the API into a relational database. From there I wanted to determine correlations between attributes and occupancy rates/total number of reservations in order to find what attributes have the greatest effect on campsite reservations. The main focus would be on creating a pipeline for importing this data and charts to illustrate the findings.

## Repo Instructions
In order to replicate the results in this readme, run the files in the src folder in the following order:
    
    1. mongo.py
    2. pandas_cleaning.py
    3. postgres.py
    4. data_analysis.py
You will need to generate your own API key from recreation.gov. Running the files in that order will query the api, put the results into a MongoDB database, clean attribute names to make them suitable for importing into Postgres

## Strategy

## Data Sources
The data came from two main channels. First the reservation data for 2018 was supplied as a single CSV. It was 3,299,805 rows (invidual reservations) bu 57 columns (attributes). The file was large enough (1.72GB CSV) that I ended up using Spark and SQL to group orders by campsite id's and returned the count for each id. I then exported this new smaller dataframe to a csv for importing into Postgres.

        Sample reservation data
        +--------+-----------------+
        |EntityID|reservation_count|
        +--------+-----------------+
        |     296|              167|
        |     467|              164|
        |    3959|              155|
        |    1090|              124|
        |   29573|              124|
        +--------+-----------------+

The second set of data came from Recreation.gov's API. Each campsite call gave a combination of 11 consistent, structured 



## Data Wrangling

## Results

![8 traits vs popularity](img/traits_ranked_by_popularity.png "8 traits vs popularity")

1) Yosemite
2) Zion
3) Grand Canyon
4) Yosemite
5) Sawnee Campground - Georgia

## Summary


##