# Local Imports
from .batched_queue import BatchedQueue

from urllib.parse import urlparse, urlunparse, urljoin
from collections import defaultdict

import logging


logger = logging.getLogger("SCRAPER")



def order_urls(urls, batch_size):
    """
    Order the urls as shown below, then creates a batch of these urls
    - website1, website2, website3, website1, website2, website3
    """
    batched_urls = None

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
        batched_urls = BatchedQueue(ordered_urls, batch_size)

    except Exception as error:
        logger.error(error)
        
    finally:
        return batched_urls



def extract_website_name_from_url(url):
    """
    Extracts the website name from a url
    """
    website_name = "error-in-extracting-website-name"

    try:
        domain = urlparse(url).netloc
        # Split the domain into subdomains and domain parts
        _, _, main_domain = domain.partition('.')
        # Split the main domain into parts based on dots
        domain_parts = main_domain.split('.')
        website_name = domain_parts[0]

    except Exception as error:
        logger.error(error)

    finally:
        return website_name



def extract_base_url_from_url(url):
    base_url = "https://error-in-extracting-base-url"

    try:
        # Parse the URL
        parsed_url = urlparse(url)
        # Construct the base URL
        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"

    except Exception as error:
        logger.error(error)

    finally:
        return base_url



def fix_url(url, root_url):
    try:
        # Parse the root URL
        parsed_root = urlparse(root_url)
        
        # Parse the URL to fix
        parsed_url = urlparse(url)
        
        # If the URL is already valid (contains scheme and netloc), return it as-is
        if parsed_url.scheme and parsed_url.netloc:
            return url
        
        # Handle URLs starting with '//'
        if url.startswith('//'):
            return urlunparse((parsed_root.scheme, url[2:], parsed_url.path, parsed_url.params, parsed_url.query, parsed_url.fragment))
        
        # Handle absolute paths
        if url.startswith('/'):
            return urlunparse((parsed_root.scheme, parsed_root.netloc, url, parsed_url.params, parsed_url.query, parsed_url.fragment))
        
        # Handle relative paths
        fixed_url = urljoin(root_url, url)
        
        return fixed_url
    
    except Exception as e:
        print(f"Error fixing URL: {e}")
        return None