#!/usr/bin/env python
# coding: utf-8


import time
import random
import requests
import os


from bs4 import BeautifulSoup
from datetime import datetime

from google.cloud import bigquery
from google.oauth2 import service_account

import socket



def get_location_from_url(location_url) : 
    # Extract the city name from the base_url
    location = location_url.split("//")[1].split(".")[0]

    return(location)
    
    
def get_listing_urls(page_source, location) :
    
    soup = BeautifulSoup(page_source, "html.parser")

    # Find all <a> elements with class="titlestring" and an href attribute
    elements = soup.find_all("a", class_="posting-title", href=True)

    # Extract the href attribute from each element and store them in the "urls" list
    # Ignores ones that are in the 'search wider area' section of page by checking 
    # for city name
    urls = [element["href"] for element in elements if location in element["href"]]
    
    return(urls)

    
def dedupe_links(links,recent_links) :
    # Remove links if they're in recent links. 
    
    links = [link for link in links if link not in recent_links]
    
    return(links)

def upload_to_gbq(client,data,dataset_id,table_name) :

    if not data : 
        return(0)

    table_ref = client.dataset(dataset_id).table(table_name)
    table = client.get_table(table_ref)
    errors = client.insert_rows_json(table, data)

    if errors == []:
        print(f"{len(data)} row inserted successfully to {table_name}.")
    else:
        print("Errors occurred while inserting rows:", errors)

    return(len(data))
    
    
def get_recent_pulls(client) :
    
    # 
    # Now I'm going to just put all the links in a set rather than
    # splitting them by location. Upper bound on size is about 6K*30 < 200K 
    # so it should be fine. 
    
    query = """
        SELECT location, url 
        FROM `car-buying-272019.car_buying.raw_listing_pages` 
        WHERE datetime_pulled > DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    """

    query_job = client.query(query,
                             location = "US")

    rows = query_job.result()  # Wait for the job to complete.

    pulls = set()

    for row in rows : 
        location, url = row
        pulls.add(url)


    query = """
        SELECT DISTINCT url 
        FROM `car-buying-272019.car_buying.links_need_harvesting` 
    """

    query_job = client.query(query,
                             location = "US")

    rows = query_job.result()  # Wait for the job to complete.

    for row in rows : 
        pulls.add(row[0])
    
    return(pulls)
    
def get_all_locations() :
    # URL for all Craigslist locations
    base_url = "https://geo.craigslist.org/iso/us"
    
    # Fetch the page with all Craigslist locations
    response = requests.get(base_url)
    
    # Parse the page with BeautifulSoup
    soup = BeautifulSoup(response.content, "html.parser")
    
    # Find the container with all location links
    container = soup.find('div', class_='geo-site-list-container')
    
    # Find all <a> elements within the container and extract hrefs (which 
    # should be URLs for Craigslist websites)
    location_urls = [element["href"] for element in container.find_all("a", href=True)]
    locations = {get_location_from_url(url): url for url in location_urls}

    return(locations)    


def main() : 

    start = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # Get all the locations. At some point maybe we store this and update 
    # it? Maybe it doesn't matter, since it runs instantly.
    locations = get_all_locations()

    # Now let's go get all the listings we've found over the last month. 
    # That will prevent us from pulling duplicates. 

    # dotenv stuff
    service_path = os.getenv("SERVICE_PATH")
    service_file = os.getenv("SERVICE_FILE")     
    gbq_proj_id = os.getenv("GBQ_PROJECT_ID") 
    dataset_id = os.getenv("GBQ_DATASET_ID")

    credentials = service_account.Credentials.from_service_account_file(service_path + service_file)

    client = bigquery.Client(credentials = credentials, project=gbq_proj_id)    
    
    last_month_pulls = get_recent_pulls(client)    
    total_listing_links = 0


    # Now we'll iterate over the locations. For each location we'll do the following: 
    # 
    # 1. Get the links from the last day.
    # 2. Ignore any that are duplicates for that location
    # 3. Load the links into `links_need_harvesting`

    service = ChromeService()
    
    # Set options
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument("--no-sandbox")

    
    for idx, location in enumerate(locations) :
        location_url = locations[location]
        
        # Drivers seem to get disconnected after a while, so we'll reconnect for each 
        # location. 
        driver = webdriver.Chrome(service=service, options=options)
        
        location_links = list()
        
        # Append the base query to the city URL
        base_url = location_url + "/search/cta?bundleDuplicates=1&postedToday=1&purveyor=owner"
        page = 0 
        
        while True : 
            # Append the page number to the URL
            request_url = base_url + "#search=1~gallery~" + str(page) + "~0"
            
            print(f"requesting {request_url}")
            
            driver.get(request_url)
    
            time.sleep(1 + random.random())
    
            links = get_listing_urls(driver.page_source, location)
            
            print(f"Returned {len(links)} links")
            
            if not links or (set(links).issubset(set(location_links))):
                break
            else : 
                location_links.extend(links)        
                
            print(f"Total links: {len(location_links)}")
    
            page += 1
            
            time.sleep(random.random() + 0.25)
                
        
        # De-dupe links
        location_links = dedupe_links(location_links,last_month_pulls)
        print(f"Post de-duping we have {len(location_links)} total links.")
        
        total_listing_links += len(location_links)
        
        # upload links
        upload_data = list()
        
        for link in location_links : 
            row_data = {
                "url": link,
                "location": location,
                "row_created": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            upload_data.append(row_data)
            
        upload_to_gbq(client,upload_data,dataset_id,'links_need_harvesting')
                    
        print("-"*45)
        driver.quit()
        
#        if idx > 10 :
#            break
    
    
    log_row = [{
        'task' : "getting_links",
        'time_started' : start,
        'time_finished' : datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        'notes' : f"pulled {total_listing_links} links on {hostname}."
    }]
    
    upload_to_gbq(client,log_row,dataset_id,'log')


    client.close()


if __name__ == '__main__':
    main()
