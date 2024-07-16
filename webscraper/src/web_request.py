from fake_headers import Headers

import tls_client
import logging
import aiohttp
import asyncio

logger = logging.getLogger("SCRAPER")


def headers():
    """
    Generate random headers
    """
    header = Headers(headers=True).generate()

    return header



async def check_response_status(status_code, url):
    if status_code in [302, 303, 500, 502, 503]:
        # These status codes are due to the server not the request
        logger.warning(f"({url}), Response Status Code {str(status_code)}: This status code us due to the server not the request")
        return None

    elif status_code in [301, 308, 400, 404, 410]:
        # These status codes indicate the resource has been moved or there was a bad request
        logger.warning(f"({url}), Response Status Code {str(status_code)}. This status code indicates the resource has been moved or there was a bad request")
        return None
    
    elif status_code in [403]:
        # 403 is a forbidden error
        logger.warning(f"({url}), Response Status Code {str(status_code)}")
        return None

    elif status_code != 200:
        # Any other error should be logged so it can be handled in the future
        logger.warning(f"({url}), Response Status Code {str(status_code)}")
        return None
    
    else:
        return True



async def tls_client_request(urls):
    try:
        with tls_client.Session(client_identifier="chrome112", random_tls_extension_order=True) as session:
            tasks = [await tls_client_fetch(url, session) for url in urls]
            return tasks

    except Exception as error:
        logger.error("Failed tls_client.Session()", error)


async def tls_client_fetch(url, session):
    try:
        response = session.get(url, headers=headers())
        status = response.status_code
        if await check_response_status(status, url):
            return response.content
        return {"status": status}

    except Exception as error:
        logger.error(f"Failed request for ({url})", error)


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
        logger.error("Failed aiohttp.ClientSession()", error)



async def aiohttp_fetch(url, session) -> None:
    """
    Asynchronously fetches content from a URL using aiohttp.

    Args:
        url (str): URL to fetch.
        session (aiohttp.ClientSession | tls_client.Session): Session for the HTTP GET request.
    
    Returns:
        Response text on success, or HTTP status code on failure.
    """
    async with session.get(url, headers=headers()) as response:
        status = response.status
        if await check_response_status(status, url):
            return await response.text()
        return {"status": status}