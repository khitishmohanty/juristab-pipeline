import pytest
from utils.common import get_page_from_url

@pytest.mark.parametrize("url, expected_page", [
    ("http://example.com/search?page=5", 5),
    ("http://example.com/search?name=test&page=2", 2),
    ("http://example.com/search?page=invalid", 1)
])
def test_get_page_from_url(url, expected_page):
    page = get_page_from_url(url)
    assert page is not None
    assert page == expected_page
