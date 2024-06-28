# Local Imports
from src.processors import *

from bs4 import BeautifulSoup

import concurrent.futures
import time



async def run(urls, scraping_config, batch_size=10):
    try:
        # Split the scraping config into the config data for locating items
        # and the config data for scraping the item pages
        locate_config, page_config = setup_scraping_config(scraping_config)
        
        # Split the urls into item locate urls and item page urls
        locate_urls, page_urls = order_urls(urls, locate_config, batch_size)
        
        # Scrape all the item for the given locate_urls
        located_items = await scrape_batches(locate_urls, locate_config)

        # Scrape all the item for the given locate_urls
        page_items = await scrape_batches(page_urls, page_config)

        return {"item-page": page_items, "item-locate": located_items}

    except Exception as error:
        print("run", error)



async def scrape_batches(queue, scraping_config, batch_delay_seconds=5):
    """
    This function is designed to be an asynchronous task that continuously pops batches of URLs
    from the provided BatchedQueue and sends requests to the urls asynchronously using aiohttp.
    A delay is introduced between each batch processing cycle to control the rate of URL processing.
    The responses from asynchronous requests are then handed off to a ThreadPoolExecutor for
    parallelized CPU-bound scraping tasks, performed by the 'scrape' function.

    Args:
        queue (BatchedQueue): An instance of BatchedQueue containing URLs to be processed.
        batch_delay_seconds (int, optional): The delay in seconds between processing batches. Default is 10 seconds.

    Returns:
        List: A list containing the results collected by the 'scrape' function for each processed batch.
    """
    results = {}
    try:
        # Continuously process batches until the queue is empty
        while queue.length > 0:
            batch_urls = queue.pop()

            responses = await aiohttp_request(batch_urls)

            # Prepare the arguments for the scrape function
            scrape_args = []
            for response, url in zip(responses, batch_urls):
                website_name = extract_website_name_from_url(url)
                config = scraping_config.get(website_name)
                scrape_args.append((config, response, url))

            # Use ThreadPoolExecutor to parallelize the CPU-bound scraping task
            with concurrent.futures.ThreadPoolExecutor() as executor:
                batch_results = list(executor.map(lambda args: scrape(*args), scrape_args))

            for result in batch_results:
                results.update(result)

            if queue.length > 0:
                time.sleep(batch_delay_seconds)


    except Exception as error:
        print("scrape_batches", error)
    
    finally:
        return results



def scrape(*args):
    # scraped_data = {"url1": {"key1": "data1", "key2": "data2"}}
    scraped_data = {}
    try:
        config, response, url = args

        # When a request has faild the status is returned
        if isinstance(response, dict):
            scraped_data = {url: {"status": response.get("status")}}

        html = BeautifulSoup(response, "lxml")
        
        scraped_data[url] = scrape_item(html, config.get("scraping-config"), config.get("root"))

        
    except Exception as error:
        scraped_data = {url: {"error": str(error)}}
        print("scrape", error)

    finally:
        return scraped_data



def scrape_item(html, config, root_url):
    scraped_data = {}
    try:
        root_item_name = config.get("item")
        item_elements = scrape_config(html, config)
        item_attr_type = config.get("config")[-1].get("attr")

        # The attribute type is describing what value to scrape from an element
        # for example the elements text, href, src etc.
        # If it doesn't exist in the config then the item has sub-items
        if item_attr_type is None:
            scraped_data[root_item_name] = []
            for element in item_elements:
                element_data = {}

                # Iterate through the sub-items and scrape their data
                for sub_item in config.get("sub-items"):
                    sub_item_name = sub_item.get("item")
                    element_data[sub_item_name] = scrape_item(element, sub_item, root_url)

                scraped_data[root_item_name].append(element_data)

        elif item_attr_type == ".text":
            scraped_data = item_elements[0].get_text(strip=True)
        
        elif item_attr_type in ["href", "src"]: 
            scraped_data = fix_url(item_elements[0][item_attr_type], root_url)
        else:
            scraped_data = item_elements[0][item_attr_type]

    except Exception as error:
        print("scrape_item", error)

    finally:
        return scraped_data
    


def get_element_config(element):
    # The tag, and attributes for the element
    tag = element["tag"]
    attr_name, attr_value = list(element.items())[1]
    attrs = {attr_name: attr_value}

    return tag, attrs



def scrape_config(html, config):
    """
    Loops through the config list and scrapes the elements. 
    If the config dict contains a max-items key then the last item in the config list
    will be scraped for this many times.

    Args:
        html (Beautifulsoup): HTML the elements will be scraped from
        config (list): The config information for how to locate an element

    Returns:
        html (list[Beautifulsoup] | None): The final scraped element(s). Returns None if element not found.
    """
    try:
        element_config = config.get("config")
        max_items = config.get("max-items")
        
        for element in element_config[:-1]:
            html = html.find(*get_element_config(element))
            if html is None: 
                return None
        
        last_element_config = get_element_config(element_config[-1])
        if max_items is not None:
            elements = html.find_all(*last_element_config, limit=max_items)
        else:
            elements = [html.find(*last_element_config)]


    except Exception as error:
        print("scrape_config", error)
        elements = None

    finally:
        return elements