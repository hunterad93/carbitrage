#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jul  1 14:18:58 2023

@author: chandler

This file handles the harvesting of the HTML pages and cleaning up 
the "links need harvesting" table. 
"""

import time
from datetime import datetime
import random
import requests
import socket
import os


from google.cloud import bigquery
from google.oauth2 import service_account

def get_urls_to_harvest(client) : 
    
    query = """
        SELECT url, location
        FROM `car-buying-272019.car_buying.links_need_harvesting` 
        WHERE url NOT IN (
            SELECT DISTINCT url 
            FROM `car-buying-272019.car_buying.raw_listing_pages`)
        ORDER BY row_created ASC
        LIMIT 130
        """

    query_job = client.query(query,
                             location = "US")
    
    rows = query_job.result()  # Wait for the job to complete.
    
    urls = []
    
    for row in rows : 
        url, location = row
        
        urls.append((url,location))
        
    return(urls)
    

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

def delete_harvested_links(client, pulled_links) :
    # once we've requested the pages, we remove the links 
    # from the "links needing harvesting" table
    
    
    query = """
    DELETE FROM `car-buying-272019.car_buying.links_need_harvesting` 
    WHERE url IN (
      SELECT url FROM `car-buying-272019.car_buying.links_need_harvesting`
      WHERE url IN UNNEST(@pulled_links)
    )
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("pulled_links", "STRING", pulled_links)
        ]
    )
    
    try:
        query_job = client.query(query, job_config=job_config)
        query_job.result()  # Wait for the query to complete
        print("Deletion completed successfully.")
    except Exception as e:
        print("Error occurred during deletion:", str(e))




def main() : 
    
    start = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    
    # dotenv stuff
    service_path = os.getenv("SERVICE_PATH")
    service_file = os.getenv("SERVICE_FILE")     
    gbq_proj_id = os.getenv("GBQ_PROJECT_ID") 
    dataset_id = os.getenv("GBQ_DATASET_ID")

    
    credentials = service_account.Credentials.from_service_account_file(service_path + service_file)
    
    client = bigquery.Client(credentials = credentials, project=gbq_proj_id)    
    
    links = get_urls_to_harvest(client)
    
    if len(links) > 0 : 
    
        table_id = "car-buying-272019.car_buying.raw_listing_pages"
    
        rows_to_insert = []
        
        for idx, link_tuple in enumerate(links) : 
            link, location = link_tuple
            
            response = requests.get(link)
            time.sleep(1 + random.random())
        
            listing_page = response.text
            current_datetime = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")    
        
            rows_to_insert.append({
                "url": link,  
                "datetime_pulled": current_datetime,
                "raw_html": listing_page,
                "location": location  
            })
        
        
        
        errors = client.insert_rows_json(table_id, json_rows=rows_to_insert)
            
        if errors == []:
            print(f"{len(rows_to_insert)} rows inserted successfully in raw_listing_pages.")
        else:
            print("Errors occurred while inserting rows:", errors)
        
        
        
        log_row = [{
            'task' : "harvesting_pages",
            'time_started' : start,
            'time_finished' : datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            'notes' : f"harvested {len(rows_to_insert)} pages on {hostname}"
        }]
        
        upload_to_gbq(client,log_row,dataset_id,'log')
        
    return(0)


if __name__ == '__main__':
    main()

