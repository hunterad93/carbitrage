# This file processes the raw listing pages. 

from datetime import datetime
import re 
from collections import defaultdict
from bs4 import BeautifulSoup
import random
from dateutil import parser # pip install python-dateutil 
import json


import os

from google.cloud import bigquery
from google.oauth2 import service_account

from fuzzywuzzy import fuzz
from fuzzywuzzy import process

def get_processed_needs_basic(client) : 
  """
  Query the processed_listing_pages and gather all URLs that have `needs_basic_parsing` set to true.
  """

  query = """
          SELECT
            DISTINCT url
          FROM
          `car-buying-272019.car_buying.processed_listing_pages`
          WHERE 
            needs_basic_parsing IS TRUE;
        """
        
  query_job = client.query(query,
                          location = "US")

  rows = query_job.result()  # Wait for the job to complete.

  results = []

  for row in rows : 
    results.append(row[0])

  return(results)

def get_raw_needs_parsing(client) : 
  """
    Query the raw_listing_pages and gather all URLs that aren't in processed.
  """

  query = """
          SELECT
            DISTINCT url
          FROM
          `car-buying-272019.car_buying.raw_listing_pages`
          WHERE 
            url NOT IN (
              SELECT DISTINCT url 
              FROM
              `car-buying-272019.car_buying.processed_listing_pages`
              )
        """
          
  query_job = client.query(query,
                              location = "US")

  rows = query_job.result()  # Wait for the job to complete.

  results = []
  for row in rows : 
      results.append(row[0])

  return(results)

def get_make_models(client) : 
  """"
    This function returns a dictionary with three keys: makes, models, and pairs
  """

  query = """
          SELECT
            make, model, short_model
          FROM
          `car-buying-272019.car_buying.make_model_year`
        """
        
  query_job = client.query(query,
                            location = "US")

  rows = query_job.result()  # Wait for the job to complete.

  results = defaultdict(set)

  # TODO: undoubtedly there's a smarter way to handle these models
  # than just dumping everything in a list. Seems like the regular
  # model should take precedence. So, something where we look for
  # model first, then look for short_model.  
  for row in rows : 
      make, model, short_model = row
      results[make.lower()].add(model.lower())
      results[make.lower()].add(short_model.lower())

  return(results)

def find_best_match(input_str, targets, score_cutoff=90, top_n=3):
    # Check for exact match first
    if input_str in targets:
        return [input_str]
    
    # Use fuzzy matching if no exact match is found
    # Try multiple scorers and return the top n matches
    scorers = [fuzz.ratio, fuzz.partial_ratio, fuzz.token_sort_ratio, fuzz.token_set_ratio]
    best_matches = []
    
    for scorer in scorers:
        matches = process.extract(input_str, targets, scorer=scorer, limit=top_n)
        matches = [match for match in matches if match[1] >= score_cutoff]
        best_matches.extend(matches)

    # Flatten the list of best matches and remove duplicates
    best_matches = list(set([match[0] for match in best_matches]))

    return best_matches
        
def get_post_id(url) : 
  """ Pull the post ID out of a listing URL"""
  # Extracts the ID as the last segment of the URL, remove .html
  post_id = url.split('/')[-1].split('.')[0]  
  return(post_id)

def get_time_posted(soup) : 

  time_tag = soup.find('time', class_='date timeago')
  posted_time = None
  
  # TODO: do we need to use parser here? I'd think this is a 
  # standardized format
  if time_tag:
      posted_time = parser.parse(time_tag.get('datetime'))
      posted_time = posted_time.strftime('%Y-%m-%d %H:%M:%S')  
      # Format the datetime object as a string in BigQuery TIMESTAMP format

  return(posted_time)

def get_listing_name(soup): 
    # Extract make and model
    make_model_tag = soup.find('a', class_='valu makemodel')
    if make_model_tag:
        make_model = make_model_tag.text.strip()
        return make_model
    return None

def get_year(soup): 
    year_tag = soup.find('span', class_='valu year')
    if year_tag:
        year = year_tag.text.strip()
        return int(year)
    return None

def correct_make(text) : 
  """
    There are some common misspellings that we'll take care of. 
    This assumes we'll be in lowercase from this point on, so 
    we'll cast in here to make the code simpler. 
  """

  # TODO: 
  # - people put "benz" when they mean "mercedes benz"

  subs = {"chevy":"chevrolet",
          "cheverolet":"chevrolet",
          "mercedez":"mercedes",
          "vw":"volkswagen",
          "volkswagon":"volkwagen",
          "volkwagen":"volkswagen",
          "infinity":"infiniti",
          "chysler":"chrysler"
          }
  if text is None:
    return None

  text = text.lower()

  for typo, model in subs.items() : 
     if typo in text : 
        text = text.replace(typo,model)
        break
     
  return(text)

def correct_model(text) : 
   
  """
    Currently a placeholder

    There are some common misspellings that we'll take care of. 
    This assumes we'll be in lowercase from this point on, so 
    we'll cast in here to make the code simpler. 
  """

  # TODO: 
  # - TK

  subs = {"oddysey":"odyssey",}

  if text is None:
      return None

  text = text.lower()

  for typo, model in subs.items() : 
     if typo in text : 
        text = text.replace(typo,model)
        break

  return(text)

def get_make(soup, name, makes) : 
  """
    Extract the car model from the HTML page. Passing in anything we might
    use so that I can make this more sophisticated later. Current plan: 
    remove the year from the name, split on tokens. Look for n-grams 
    that are in our list of car models. 

    TODO: Don't need to remove the years here....
  """

  make = None

  if name: 
    name = correct_make(name)
    name = name.strip().lower()

    for this_make in makes : 
      if this_make in name or this_make.replace("-"," ") in name : 
        return(this_make)

  title = get_title_text(soup)

  if title : 

    title = correct_make(title)
    title = title.strip().lower()

    for this_make in makes : 
      if this_make in title or this_make.replace("-"," ") in title : 
        return(this_make)
  
#  if not make :       
#    print(title)

  return(make)

def get_model(make, soup, name, models) : 
  """
    Similar to the above, this attempts to extract the model from 
    the listing. For now I'll require a match, but another
    idea would be to remove year and make from the name and then
    just record whatever is left over as the model. 
  """

  model = None

  if name: 
    name = correct_model(name)
    name = name.strip().lower()

    for this_model in models : 
      if this_model in name or this_model.replace("-"," ") in name : 
        return(this_model)

  title = get_title_text(soup)

  if title : 

    title = correct_model(title)
    title = title.strip().lower()

    for this_model in models : 
      if this_model in title or this_model.replace("-"," ") in title : 
        return(this_model)



  return(model)


def parse_attrgroup(soup):
  
  attributes = {}

  try:
      # Extract attributes
      attrgroup = soup.find_all('div', class_='attr')
      for attr in attrgroup:
          # Find the label and value spans within each attribute div
          label_span = attr.find('span', class_='labl')
          value_span = attr.find('span', class_='valu')
          if label_span and value_span:
              # Extract text from spans and use as key-value pair in attributes dictionary
              key = label_span.text.strip().rstrip(':')
              value = value_span.text.strip()
              attributes[key] = value

  except Exception as e:
      print(f"Error parsing attribute group: {e}")

  # Check for 'odometer' attribute to adjust its value if necessary
  if 'odometer' in attributes:
     try:
         if float(attributes['odometer']) < 1000:
            attributes['odometer'] = float(attributes['odometer']) * 1000
     except ValueError:
         # Handle case where odometer value is not a float
         print(f"Error converting odometer value to float: {attributes['odometer']}")

  return(attributes)

def get_lat_long(soup) : 
   
  latitude = None
  longitude = None

  script_tag = soup.find('script', {'id': 'ld_posting_data'})

  if script_tag is not None:
      json_data = json.loads(script_tag.string)
      offers = json_data.get('offers')
      if offers:
          availableAtOrFrom = offers.get('availableAtOrFrom')
          if availableAtOrFrom and 'geo' in availableAtOrFrom:
              latitude = availableAtOrFrom['geo'].get('latitude')
              longitude = availableAtOrFrom['geo'].get('longitude')

  return((latitude,longitude))


def get_description(soup) : 
  description = None

  script_tag = soup.find('script', {'id': 'ld_posting_data'})
  
  if script_tag is not None:
    json_data = json.loads(script_tag.string)
    description = json_data.get('name') + '>>>' + json_data.get('description')

  return(description)

def get_title_text(soup) : 
  title_text = None
  span_element = soup.find('span', id='titletextonly')

  # Check if the element is found
  if span_element:
    title_text = span_element.text

  return(title_text)

def get_posting_body(soup) : 
  posting_body = None
  if soup : 
    section_element = soup.find('section', id='postingbody')

    if section_element : 
      posting_body = section_element.text

  return(posting_body)

def get_price(soup) : 
  price = None

  script_tag = soup.find('script', {'id': 'ld_posting_data'})
  if script_tag is not None:
    json_data = json.loads(script_tag.string)
    offers = json_data.get('offers')
    if offers:
      price = offers.get('price')

  return(price)   

def get_num_images(soup) : 
  num_images = None
  
  span_element = soup.find('span', class_='slider-info')
  if span_element:
    image_info = span_element.text
    # Extracting the number from the text (assuming the format is "image X of Y")
    num_images = int(image_info.split()[-1])

  return(num_images)

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

def main() : 

  start = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

  # dotenv stuff
  service_path = os.getenv("SERVICE_PATH")
  service_file = os.getenv("SERVICE_FILE")     
  gbq_proj_id = os.getenv("GBQ_PROJECT_ID") 
  dataset_id = os.getenv("GBQ_DATASET_ID")



  credentials = service_account.Credentials.from_service_account_file(service_path + service_file)

  bq_client = bigquery.Client(credentials = credentials, project=gbq_proj_id)   

  # Grab our makes and models
  make_2_models = get_make_models(bq_client) 

  # Grab the URLs we're going to parse. 
  urls = get_processed_needs_basic(bq_client)
  urls.extend(get_raw_needs_parsing(bq_client))
  urls = list(set(urls))

  print(f"We have {len(urls)} that need processing.")

  url_limit = 2500
  rows = {}

  if len(urls) > url_limit : 
    # Select a set to parse
    random.seed(20230806)
    urls_to_process = random.sample(urls,k=url_limit)

  elif len(urls) == 0 :
    return(0)
  else : 
     urls_to_process = urls

  query = f"""
      SELECT url, raw_html, location
      FROM `car-buying-272019.car_buying.raw_listing_pages`
      WHERE url IN UNNEST(@urls_to_process)
  """

  # Set up the query parameters
  query_params = [bigquery.ArrayQueryParameter("urls_to_process", "STRING", urls_to_process)]

  # Run the query and get the results
  query_job = bq_client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=query_params))
  results = query_job.result()

  rows_to_upload = []

  # Process and print the results
  for row in results:
    url, raw_html, location = row

    soup = BeautifulSoup(raw_html,'html.parser')

    # We'll extract all of our row elements in stand-alone functions to allow
    # for some fine tuning. The exception will be the attribute group, since
    # from those we'll just pull whatever the poster has filled out. 
    parsed_attributes = parse_attrgroup(soup)

    # Append the extracted data to the DataFrame
    extracted_data = {
        'url': url,
        'location':location, 
        'odometer': parsed_attributes.get('odometer'),
        'title': parsed_attributes.get('title status'),
        'paint': parsed_attributes.get('paint color'),
        'drive': parsed_attributes.get('drive'),
        'cylinders': parsed_attributes.get('cylinders'),
        'condition': parsed_attributes.get("condition"),
        'fuel': parsed_attributes.get('fuel'),
        'type': parsed_attributes.get('type'),
        'transmission': parsed_attributes.get('transmission'),
        'vin':parsed_attributes.get('VIN')
    }

    car_name = get_listing_name(soup)
    #print(f"Car name is '{car_name}'")
    extracted_data['name'] = car_name

    extracted_data['post_id'] = get_post_id(url) 
    extracted_data['time_posted'] = get_time_posted(soup)

    extracted_data['year'] = get_year(soup)

    # Price
    extracted_data['price'] = get_price(soup)

    # Posting body text
    extracted_data['posting_body_text'] = get_posting_body(soup)

    # title text
    extracted_data['title_text'] = get_title_text(soup)

    # num images
    extracted_data['num_images'] = get_num_images(soup)

    # lat/long
    holder = get_lat_long(soup) 
    extracted_data['latitude'] = holder[0]
    extracted_data['longitude'] = holder[1]

    # make and model
    make = get_make(soup,car_name,make_2_models.keys())

    extracted_data['make'] = make

    model = None
    if make and make in make_2_models: 
      model = get_model(make,
                        soup,
                        car_name,
                        make_2_models[make])



    extracted_data['model'] = model

    # our flags
    extracted_data['needs_basic_parsing'] = False # TODO: Worth adding a check here? 
    extracted_data['basic_processed_time'] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    if model and make and extracted_data['price'] : 
      extracted_data['needs_ai_parsing'] = False
    else : 
       extracted_data['needs_ai_parsing'] = True

    extracted_data['ai_processed_time'] = None
   

    rows_to_upload.append(extracted_data)

  # Insert the batch
  upload_to_gbq(bq_client,rows_to_upload,dataset_id,'processed_listing_pages')

  # Now log it.
  log_row = [{
     'task' : "basic_html_parsing",
     'time_started' : start,
     'time_finished' : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
     'notes' : f"Processed {len(rows_to_upload)} HTML pages on {hostname}."
     }]

  upload_to_gbq(bq_client,log_row,dataset_id,'log')

  return(0)


if __name__ == '__main__':
    main()

