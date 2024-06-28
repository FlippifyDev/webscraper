from fake_headers import Headers



def headers():
    """
    Generate random headers
    """
    header = Headers(headers=True).generate()

    return header



async def check_response_status(status_code, url):
    if status_code in [302, 303, 500, 502, 503]:
        # These status codes are due to the server not the request
        print(f"({url}), Response Status Code {str(status_code)}: This status code us due to the server not the request")
        return None

    elif status_code in [301, 308, 400, 404, 410]:
        # These status codes indicate the resource has been moved or there was a bad request
        print(f"({url}), Response Status Code {str(status_code)}. This status code indicates the resource has been moved or there was a bad request")
        return None
    
    elif status_code in [403]:
        # 403 is a forbidden error
        print(f"({url}), Response Status Code {str(status_code)}")
        return None

    elif status_code != 200:
        # Any other error should be logged so it can be handled in the future
        print(f"({url}), Response Status Code {str(status_code)}")
        return None
    
    else:
        return True



async def aiohttp_fetch(url, session) -> None:
    """
    Asynchronously fetches content from a URL using aiohttp.

    Args:
        url (str): URL to fetch.
        session (aiohttp.ClientSession): Session for the HTTP GET request.
    
    Returns:
        Response text on success, or HTTP status code on failure.
    """
    async with session.get(url, headers=headers()) as response:
        status = response.status
        if await check_response_status(status, url):
            return await response.text()
        return {"status": status}