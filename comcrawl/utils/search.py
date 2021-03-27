"""Search Helpers.

This module contains helper functions for
searching through Common Crawl Indexes.

"""

import json
import requests
from ..types import ResultList, IndexList
from .multithreading import make_multithreaded

URL_TEMPLATE = ("https://index.commoncrawl.org/"
                "CC-MAIN-{index}-index?url={url}&output=json{page}")


def search_single_index(index: str, url: str, page: int) -> ResultList:
    """Searches specific Common Crawl Index for given URL pattern.

    Args:
        index: Common Crawl Index to search.
        url: URL Pattern to search.

    Returns:
        List of results dictionaries found in specified Index for the URL.

    """
    results: ResultList = []

    page = "" if page < 0 else ("&page={page}").format(page=page)

    url = URL_TEMPLATE.format(index=index, url=url, page=page)
    response = requests.get(url)

    if response.status_code == 200:
        results = [
            json.loads(result) for result in response.content.splitlines()
        ]

    return results


def search_multiple_indexes(url: str,
                            indexes: IndexList,
                            page: int = -1,
                            threads: int = None) -> ResultList:
    """Searches multiple Common Crawl Indexes for URL pattern.

    Args:
        url: The URL pattern to search for.
        indexes: List of Common Crawl Indexes to search through.
        threads: Number of threads to use for faster parallel search on
            multiple threads.

    Returns:
        List of all results found throughout the specified
        Common Crawl indexes.

    """
    results = []

    # multi-threaded search
    if threads:
        mulithreaded_search = make_multithreaded(search_single_index,
                                                 threads)
        results = mulithreaded_search(indexes, url, -1)

    # single-threaded, single-page search
    elif len(indexes) == 1:
        index_results = search_single_index(indexes[0], url, page)
        results.extend(index_results)

    # single-threaded search
    else:
        for index in indexes:
            index_results = search_single_index(index, url, -1)
            results.extend(index_results)

    return results
