# GCP-ETL-Testing
This project aims at testing GCP capabilities to handle classic ETL pipeline (Cloud Scheduler - Cloud Function - BigQuery)
It is also an opportunity for me to learn and practice on my side as I am following the GCP Professional Data Engineer online courses.

### Project Context
Purpose is to save into BigQuery tables on a hourly basis football odds from Pinnacle bookmaker for 7 european leagues.

### Extract Phase
Thanks to a private access to Pinnacle API, odds can be retrieved through classical HTTP GET requests.

### Transform Phase
The transform phase consists in aggregating some dates formats, this is not yet really advanced.

### Load Phase
The loading phase consists in creating (if not already existing) the BigQuery tables and load the data into it

## NB:
The code in this repo won't show all the steps needed to make it work.
Some challenges have been faced on top of the Python code itself

### Challenge 1
The Cloud function was getting blocked from the API. 
RC: The cloud function shares its IP Address with many instances
Solution: Using Cloud VPC to route all these trafics to one static IP address

### Challenge 2
Cloud VPC is not able to access public internet, so the API couldn't get called.
Solution: Adding a Cloud NAT capability on top of Cloud VPC
