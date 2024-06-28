# Local Imports
from .web_request import aiohttp_fetch
from .batched_queue import BatchedQueue

from collections import defaultdict
from urllib.parse import urlparse, urlunparse, urljoin

import aiohttp
import asyncio



async def aiohttp_request(urls):
    """
    Create an aiohttp session and send requests asynchronously to each url
    """
    try:
        async with aiohttp.ClientSession() as session:
            tasks = [aiohttp_fetch(url, session) for url in urls]
            # Use asyncio.gather to wait for all asynchronous requests to complete
            return await asyncio.gather(*tasks)
        
    except Exception as error:
        print("aiohttp_request", error)
        raise error



def setup_scraping_config(scraping_config):
    """
    Splits the scraping config into item-locate and item-page
    """
    try:
        locate_config = {}
        page_config = {}

        for config_dict in scraping_config:
            root =            config_dict.get("root")
            search_command =  config_dict.get("search-command")
            config =          config_dict.get("scraping-config")
            website_name =    extract_website_name_from_url(root)
            
            # Extracting item-locate and item-page
            item_locate =  config.get("item-locate")
            item_page =    config.get("item-page")

            # Creating dictionaries with root, search-command, and type
            locate_config[website_name] = {"root": root, "search-command": search_command, "type": config_dict.get("type"), "scraping-config": item_locate}
            page_config[website_name] =   {"root": root, "search-command": search_command, "type": config_dict.get("type"), "scraping-config": item_page}


        return locate_config, page_config
    
    except Exception as error:
        print("setup_scraping_config", error)



def extract_url_type(url, scraping_config):
    """
    Check if the search command is in the url, if it is then the url is a locate-url
    if it isn't then the url is a page-url
    """
    try: 
        website_name = extract_website_name_from_url(url)
        search_command = scraping_config.get(website_name, {}).get("search-command")

        if search_command is not None:
            if search_command in url:
                return "locate"
            else:
                return "page"

    except Exception as error:
        print("extract_url_type", error)



def order_urls(urls, locate_config, batch_size):
    """
    Orders a list of urls such that the urls from the same website are as far apart as possible. 
    For example if you have 6 urls from 3 different websites, the list will be order as below:
    - website1, website2, website3, website1, website2, website3

    Args:
        urls (list): A list of URLs to be ordered.

    Returns:
        list: The ordered list of URLs
    """
    try:
        if len(urls) == 0:
            return urls

        # Create a dictionary to group URLs by domain
        locate_url_groups = defaultdict(list)
        page_url_groups = defaultdict(list)


        for url in urls:
            domain = url.split('/')[2]  # Extracting domain from the URL
            
            url_type = extract_url_type(url, locate_config)
            if url_type == "locate":
                locate_url_groups[domain].append(url)
            elif url_type == "page":
                page_url_groups[domain].append(url)
                
        # Sort the groups based on the count of URLs in each group
        sorted_locate_groups = sorted(locate_url_groups.values(), key=len, reverse=True)
        sorted_page_groups = sorted(page_url_groups.values(), key=len, reverse=True)

        
        # Interleave the groups to form the ordered list
        locate_urls = [url for i in range(max(map(len, sorted_locate_groups))) for group in sorted_locate_groups for url in group[i:i+1]]
        page_urls =   [url for i in range(max(map(len, sorted_page_groups))) for group in sorted_page_groups for url in group[i:i+1]]

        return BatchedQueue(locate_urls, batch_size), BatchedQueue(page_urls, batch_size)

    except Exception as error:
        print("order_urls", error)



def extract_website_name_from_url(url):
    """
    Extracts the website name from a url
    """
    try:
        domain = urlparse(url).netloc
        # Split the domain into subdomains and domain parts
        _, _, main_domain = domain.partition('.')
        # Split the main domain into parts based on dots
        domain_parts = main_domain.split('.')
        return domain_parts[0]

    except Exception as error:
        print("extract_website_name_from_url", error)



def fix_url(url, root_url):
    """
    Some links retrieved from <a> tags don't include the root url, this function
    fixes this but adding the root to the url
    """
    # Check if the URL is valid
    if urlparse(url).scheme and urlparse(url).netloc:
        return url

    # If not valid, make it valid using the base_url
    parsed_base_url = urlparse(root_url)
    scheme = parsed_base_url.scheme
    netloc = parsed_base_url.netloc

    # Handle URLs starting with "//"
    if url.startswith('//'):
        return urlunparse((scheme, url[2:], '', '', '', ''))

    # Handle relative paths and absolute paths
    if url.startswith('/'):
        # Handle absolute paths
        path = url
    else:
        # Handle relative paths and join them with the base_url
        path = urljoin(root_url, url)

    # Construct the valid URL
    valid_url = urlunparse((scheme, netloc, path, '', '', ''))

    return valid_url