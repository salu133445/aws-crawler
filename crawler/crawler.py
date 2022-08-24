"""Crawler for AWS lambda."""
import lxml.html
import requests

TMP_DIR = "/tmp/crawler/"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36"
)


def handler(event=None, context=None):
    """Handler function that processes the event."""
    # Send a request without cookies
    response = requests.get(event["url"], headers={"User-Agent": USER_AGENT})

    # Return the error code if not successful
    if response.status_code != 200:
        return {"status_code": response.status_code}

    # Parse the raw HTML
    tree = lxml.html.fromstring(response.content)

    # ================ README ================
    # Please modify the following code:
    # 1. Extract desired information
    # 2. Return a JSON-serializable dictionary
    # ========================================

    # For example, get the root tag
    root_tag = tree.getroot().tag

    # For example, return the root tag (please also include the status code)
    return {
        "root_tag": root_tag,
        "status_code": 200,
    }
