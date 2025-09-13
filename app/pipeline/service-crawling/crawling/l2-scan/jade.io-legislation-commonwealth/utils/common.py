from urllib.parse import urlparse, parse_qs


def get_page_from_url(url):
    """Parses a URL to extract the 'page' query parameter."""
    try:
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)
        page = query_params.get('page', [1])[0]
        return int(page)
    except (ValueError, IndexError):
        return 1