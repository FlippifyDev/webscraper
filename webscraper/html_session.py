# Local Imports
from .src.processors import *
from .src.web_request import aiohttp_request, tls_client_request
from .src.config_logger import setup_logger

from bs4 import BeautifulSoup

import concurrent.futures
import asyncio
import signal
import time
import copy
import sys

logger = setup_logger("SCRAPER", "bot")


def signal_handler(sig, frame):
    # Used to stop the scraping when pressing (CTRL+C)
    logger.info("----- Look up terminated -----")
    sys.exit(0)



async def run_async(urls, scraping_config, batch_size=10, batch_delay_seconds=5):
    """Main function to process all batches asynchronously."""
    signal.signal(signal.SIGINT, signal_handler)
    
    queue = order_urls(urls, batch_size)
    results = {}
    
    try:
        # Process each batch of URLs until the queue is empty
        while queue.length > 0:
            logger.info(f"Batch {queue.batch_number}/{queue.size}")
            
            batch_urls = queue.pop()
            batch_request_urls = filter_urls_by_website(batch_urls)

            # Process the batch of URLs asynchronously
            responses, batch_urls_reordered = await process_batch(batch_request_urls)

            # Prepare arguments for the scrape function
            scrape_args = [
                (scraping_config[extract_website_name_from_url(url)], response, url)
                for response, url in zip(responses, batch_urls_reordered)
            ]

            # Use ThreadPoolExecutor to parallelize the scraping task
            with concurrent.futures.ThreadPoolExecutor() as executor:
                batch_results = list(executor.map(lambda args: scrape(*args), scrape_args))

            # Update the results with the batch results
            for result in batch_results:
                results.update(result)

            # Introduce a delay between processing batches
            if queue.length > 0:
                await asyncio.sleep(batch_delay_seconds)

    except KeyboardInterrupt:
        logger.info("Process interrupted by user.")
    except Exception as error:
        logger.error(f"Error occurred: {error}")
    finally:
        return results

def run(urls, scraping_config, batch_size=10, batch_delay_seconds=5):
    """Wrapper function to run the asynchronous main function."""
    # Use asyncio.run to run the async event loop
    return asyncio.run(run_async(urls, scraping_config, batch_size, batch_delay_seconds))



async def process_batch(batch_request_urls):
    """Process a batch of URLs asynchronously."""
    responses = []
    batch_urls_reordered = []
    
    for request_type, request_urls in batch_request_urls.items():
        batch_urls_reordered += request_urls
        if request_type == "aiohttp-urls":
            # Send asynchronous requests to the URLs in the current batch
            filtered_responses = await aiohttp_request(request_urls)
        elif request_type == "tls-client-urls":
            filtered_responses = await tls_client_request(request_urls)

        responses += filtered_responses

    return responses, batch_urls_reordered



def scrape(*args):
    website_config, response, url = args
    scraped_data = {url: {}}

    try:
        # When a request has faild the status is returned
        if isinstance(response, dict):
            scraped_data[url] = response
        else:
            # Parse the HTML content
            html = BeautifulSoup(response, "lxml")
            root_url = extract_base_url_from_url(url)
            items_config = website_config["config"]
            if items_config is None:
                return scraped_data

            # Scrape the elements based on the configuration
            for item_name, item_config in items_config.items():
                item_config_copy = copy.deepcopy(item_config)
                scraped_data[url].update(scrape_element_config_list(html, item_name, item_config_copy, root_url))
        
    except Exception as error:
        scraped_data = {url: {"error": str(error)}}
        logger.error(error)

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
            scraped_data = {item_name: None} 
        else:
            scraped_data[item_name] = extract_element_data(html, config.get("attr"), root_url, config.get("alt-attr"))

    except Exception as error:
        logger.error(error)

    finally:
        return scraped_data



def scrape_element_config_item(html, config):
    try:
        # Get the number of tags to scrape
        max_elements = config.get("max")
        element_index = config.get("element-index")
        
        # Get the parameters for the BeautifulSoup find methods
        soup_params = get_soup_params(config)
        
        if max_elements is None:
            # Scrape for a single item
            return html.find(*soup_params)
        else:
            if element_index is None:
                # Scrape for multiple items
                return html.find_all(*soup_params, limit=max_elements)
            else:
                # Return 
                return html.find_all(*soup_params, limit=max_elements)[element_index]

    except Exception as error:
        logger.error(f"Config: {config} |", error) 



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
        logger.error(f"Config: {item_config} |", error) 

    finally:
        return scraped_data



def extract_element_data(html, attribute, root_url, alt_attribute=None):
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
    
    except KeyError:
        if alt_attribute is not None:
            return extract_element_data(html, alt_attribute, root_url)

    except Exception as error:
        logger.error(f"Attr: {attribute} | html: {html} |", error) 



def get_soup_params(config):
    # Extract tag name and attributes from the config dictionary
    tag = config["tag"]
    attr_name, attr_value = list(config.items())[1]
    attrs = {attr_name: attr_value}

    return tag, attrs