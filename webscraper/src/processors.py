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



def order_urls(urls, batch_size):
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
        url_groups = defaultdict(list)

        for url in urls:
            domain = url.split('/')[2]  # Extract domain from the url
            url_groups[domain].append(url)
        
        # Sort the groups based on the count of URLs in each group
        sorted_groups = sorted(url_groups.values(), key=len, reverse=True)
        
        # Interleave the groups to form the ordered list
        ordered_urls = [url for i in range(max(map(len, sorted_groups))) for group in sorted_groups for url in group[i:i+1]]

        return BatchedQueue(ordered_urls, batch_size)

    except Exception as error:
        print("Couldn't process urls", error)



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



def extract_base_url_from_url(url):
    try:
        # Parse the URL
        parsed_url = urlparse(url)
        # Construct the base URL
        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
        return base_url
    except Exception as error:
        print("extract_base_url_from_url error:", error)
        return None



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