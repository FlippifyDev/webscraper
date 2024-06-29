# Local Imports
from .src.processors import *

from bs4 import BeautifulSoup

import concurrent.futures
import time
import copy



def run(urls, scraping_config, batch_size=10, batch_delay_seconds=5):
    queue = order_urls(urls, batch_size)
    results = {}
    try:
        # Process each batch of urls until the queue is empty
        while queue.length > 0:
            batch_urls = queue.pop()
            
            # Send asynchronous requests to the urls in the current batch
            responses = asyncio.run(aiohttp_request(batch_urls))

            # Prepare arguments for the scrape function
            scrape_args = [
                (scraping_config[extract_website_name_from_url(url)], response, url)
                for response, url in zip(responses, batch_urls)
            ]

            # Use ThreadPoolExecutor to parallelize the scraping task
            with concurrent.futures.ThreadPoolExecutor() as executor:
                batch_results = list(executor.map(lambda args: scrape(*args), scrape_args))

            # Update the results with the batch results
            for result in batch_results:
                results.update(result)

            # Introduce a delay between processing batches
            if queue.length > 0:
                time.sleep(batch_delay_seconds)

    except Exception as error:
        print("scrape_batches:", error)
    
    finally:
        return results



def scrape(*args):
    website_config, response, url = args
    scraped_data = {url: {}}

    try:
        # When a request has faild the status is returned
        if isinstance(response, dict):
            scraped_data[url]["status"] = response.get("status")
        else:
            # Parse the HTML content
            html = BeautifulSoup(response, "lxml")
            root_url = extract_base_url_from_url(url)
            items_config = website_config["config"]

            # Scrape the elements based on the configuration
            for item_name, item_config in items_config.items():
                scraped_data[url].update(scrape_element_config_list(html, item_name, item_config, root_url))
        
    except Exception as error:
        scraped_data = {url: {"error": str(error)}}
        print("scrape", error)

    finally:
        return scraped_data



def scrape_element_config_list(html, item_name, item_config, root_url):
    scraped_data = {}

    try:
        element_config = item_config.pop("element-config")

        # Iterate through the config data and scrape the HTML for the element
        for config in element_config:
            if html is None:
                return
            
            html = scrape_element_config_item(html, config)

        if isinstance(html, list):
            scraped_data[item_name] = handle_multiple_elements(html, item_config, root_url)
        elif html is None:
            scraped_data = None 
        else:
            scraped_data[item_name] = extract_element_data(html, config.get("attr"), root_url)

    except Exception as error:
        print("scrape_element_config_list", error)

    finally:
        return scraped_data



def scrape_element_config_item(html, config):
    try:
        # Get the number of tags to scrape
        max_elements = config.get("max")
        
        # Get the parameters for the BeautifulSoup find methods
        soup_params = get_soup_params(config)
        
        if max_elements is None:
            # Scrape for a single item
            return html.find(*soup_params)
        else:
            # Scrape for multiple items
            return html.find_all(*soup_params, limit=max_elements)

    except Exception as error:
        print("scrape_element_config_item:", error)



def handle_multiple_elements(html, item_config, root_url):
    scraped_data = []

    try:
        # Iterate over each HTML element in the list
        for element in html:
            sub_item_data = {}
            item_config_copy = copy.deepcopy(item_config)

            # Iterate over each sub-item config
            for sub_item_name, sub_item_config in item_config_copy.items():
                # Scrape data for each sub-item and update sub_item_data
                sub_item_data.update(scrape_element_config_list(element, sub_item_name, sub_item_config, root_url))
            
            # Append the scraped data for the current element to scraped_data list
            scraped_data.append(sub_item_data)
    
    except Exception as error:
        print("Error", error)   

    finally:
        return scraped_data



def extract_element_data(html, attribute, root_url):
    try:
        # Extract text content if attribute is ".text"
        if attribute == ".text":
            return html.get_text(strip=True)
        
        # Fix and return urls for "href" or "src" attributes
        elif attribute in ["href", "src"]:
            return fix_url(html[attribute], root_url)
        
        # Return the value of the specified attribute
        else:
            return html[attribute]

    except Exception as error:
        print("extract_element_data:", error)



def get_soup_params(config):
    # Extract tag name and attributes from the config dictionary
    tag = config["tag"]
    attr_name, attr_value = list(config.items())[1]
    attrs = {attr_name: attr_value}

    return tag, attrs