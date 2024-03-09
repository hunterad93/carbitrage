<div align="center">
  <a href="https://www.youtube.com/watch?v=c0oT2hzpGXo">
    <img src="https://img.youtube.com/vi/c0oT2hzpGXo/0.jpg" alt="Carbitrage Overview">
  </a>
</div>

# carbitrage

This repo contains *some* of the code for the 'carbitrage' project, which scrapes used car listing pages from Craigslist and transforms the raw listings into a table of useful data including expected prices generated through an linear model. The data for the last 45 days of listings can be viewed in a Looker dashboard [here](https://lookerstudio.google.com/reporting/15724f59-7692-4920-95ac-a2c8f76029eb/page/jUEsD).

The concept for this project was developed by John Chandler years ago, but it wasn't fully operational as an automated pipeline until August of 2023 when he and I developed the current codebase. That code has been in production scraping listings with minimal maintenance for the last 7 months. The project utilizes google cloud platform services including cloud functions, BigQuery, and Looker studio.

A private repo contains the full codebase, this repo contains the python code and SQL queries I contributed to in the project.

## Scraping Pipeline Structure

### Listing URL gathering
The first python cron job `get_links.py` operates by scraping Craigslist car and auto search pages for each craigslist subdomain (e.g./missoula.craigslist.org/search/cta), iterating through the pages of search results gathering all car listing urls for each subdomain. These urls are stored in a GBQ table - **links_need_harvesting**.

### Listing html scraping
The second cron job `harvest_pages.py` uses a query to select the set of URLS to be scraped from the url table and scrapes the raw html page source from each, storing html pages in **raw_listing_pages**.

### Cleanup function
The third cron job cleans the **links_need_harvesting** table of all successfully scraped URLs, by removing URLs present in **raw_listing_pages**.

### Raw page processing
The fourth and final cron job `process_listing_pages.py` processes the raw html pages, producing rows of data from each page including make, model, year, condition, price etc.

A database of known makes and models is compared against using a 'fuzzy match' to deal with misspellings and to reduce granularity of car model descriptions - eg Ford f150 heavy duty 4x4 etc. becomes ford f150.

## Model fitting function
An important component of carbitrage is including an expected price for each car listing to compare the actual price against. Cars priced under market expectations can be highlighted for users by adding this field.

The current approach used for this is to create separate linear models for each make and model of car in the database. The linear model is simple but fairly effective - price ~ year + log(miles) + condition. The `lm_fit_uploader_cloud_function` folder contains the .py and requirements.txt for the cloud function which fits these models.

This cloud function runs weekly, accessing the SQL view `lm_fit_vw.sql` which is a subset of clean data from the most recent 6 months of listings for linear models to be fit on. This view filters out records with missing data, outliers which are likely to be data entry errors, and car types with sample sizes too small for model fitting. This view typically contains around 350,000 records.

The model fitting function `upload_lms.py` groups `lm_fit_vw.sql` data by make and model, and fits an lm to each group. The coefficients, intercept, r-squared, and sample size are saved in a GBQ table - **lm_lookup_table**.

## User dashboard

The scheduled query `hot_deals_dashboard_vw.sql` selects the most recent 45 days of listings from processed listings, joining them with the **lm_lookup_table** and the **state_lookup_table**. Coefficients from the **lm_lookup_table** are used with appropriate listing fields to calculate expected price. The [dashboard](https://lookerstudio.google.com/reporting/15724f59-7692-4920-95ac-a2c8f76029eb/page/jUEsD) connects to this scheduled query's output which runs daily. Be wary of scams and incorrectly entered data!