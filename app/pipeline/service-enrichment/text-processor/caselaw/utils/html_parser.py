from bs4 import BeautifulSoup

class HtmlParser:
    """ A utility class to parse and extract text from HTML content. """
    
    def extract_text(self, html_content: str) -> str:
        """
        Extracts text from the given HTML content.

        Args:
            html_content (str): The HTML content to parse.

        Returns:
            str: The extracted text.
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        return soup.get_text(separator=' ', strip=True)

    