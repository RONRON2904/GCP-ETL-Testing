# GCP-ETL-Testing
This project aims at testing GCP capabilities to handle classic ETL pipeline (Cloud Scheduler - Cloud Function - BigQuery)
It is also an opportunity for me to learn and practice on my side as I am following the GCP Professional Data Engineer online courses.

# Project Context
Purpose is to save into BigQuery tables on a hourly basis football odds from Pinnacle bookmaker for 7 european leagues.

## Extract Phase
Thanks to a private access to Pinnacle API, odds can be retrieved through classical HTTP GET requests.
