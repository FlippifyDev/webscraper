# Local Imports
from .batched_queue import BatchedQueue

from urllib.parse import urlparse, urlunparse, urljoin, parse_qs, urlencode
from collections import defaultdict

import logging


logger = logging.getLogger("SCRAPER")



def filter_urls_by_website(urls):
    all_urls = {}
    tls_client_urls = []
    aiohttp_urls = []

    for url in urls:
        website_name = extract_website_name_from_url(url)
        if website_name in ["argos", "ebay"]:
            tls_client_urls.append(url)

        else:
            aiohttp_urls.append(url)

    all_urls["aiohttp-urls"] = aiohttp_urls
    all_urls["tls-client-urls"] = tls_client_urls

    return all_urls


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
    base_url = "https://error-in-fixing-url"

    try:
        # Parse the root URL
        parsed_root = urlparse(root_url)

        # Parse the URL to fix
        parsed_url = urlparse(url)

        # If the URL is already valid (contains scheme and netloc), return it as-is
        if parsed_url.scheme and parsed_url.netloc:
            return url

        # Handle URLs starting with '//'
        if url.startswith("//"):
            return urlunparse(
                (
                    parsed_root.scheme,
                    parsed_url.netloc,
                    parsed_url.path,
                    parsed_url.params,
                    parsed_url.query,
                    parsed_url.fragment,
                )
            )

        # Handle absolute paths
        if url.startswith("/"):
            return urlunparse(
                (
                    parsed_root.scheme,
                    parsed_root.netloc,
                    parsed_url.path,
                    parsed_url.params,
                    parsed_url.query,
                    parsed_url.fragment,
                )
            )

        # Handle relative paths
        fixed_url = urljoin(root_url, url)

        # Parse the fixed URL to merge query parameters
        parsed_fixed_url = urlparse(fixed_url)
        root_query_params = parse_qs(parsed_root.query)
        url_query_params = parse_qs(parsed_url.query)

        # Merge query parameters, with URL's query parameters taking precedence
        merged_query_params = {**root_query_params, **url_query_params}
        merged_query = urlencode(merged_query_params, doseq=True)


        return urlunparse(
            (
                parsed_fixed_url.scheme,
                parsed_fixed_url.netloc,
                parsed_fixed_url.path,
                parsed_fixed_url.params,
                merged_query,
                parsed_fixed_url.fragment,
            )
        )

    except Exception as error:
        logger.error(f"Couldn't fix ({url})", error)
        return base_url