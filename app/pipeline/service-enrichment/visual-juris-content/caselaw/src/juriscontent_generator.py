from bs4 import BeautifulSoup

class JuriscontentGenerator:
    """
    A utility class to parse and transform source HTML into juriscontent.html.
    """
    
    def generate(self, html_content: str) -> str:
        """
        Transforms the raw HTML content into the desired juriscontent.html format.

        This version now applies the 'Poppins' font family to the document and
        is robust against source files that are HTML fragments.

        Args:
            html_content (str): The source HTML content to parse.

        Returns:
            str: The transformed HTML content as a string.
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Remove any pre-existing script or style tags to ensure a clean slate
        for tag in soup(["script", "style"]):
            tag.decompose()
        
        # --- ADD FONT STYLING ---
        # Find the head tag, or create one if it doesn't exist
        head = soup.find('head')
        if not head:
            head = soup.new_tag('head')
            # FIX: Check if an <html> tag exists before trying to insert into it.
            # This handles cases where the source is an HTML fragment.
            if soup.html:
                soup.html.insert(0, head)
            else:
                # If no <html> tag, insert the head at the top of the fragment.
                soup.insert(0, head)

        # Create a new <style> tag to add the Poppins font
        style_tag = soup.new_tag('style')
        # Import Poppins from Google Fonts and set it as the default for the body
        style_tag.string = """
            @import url('https://fonts.googleapis.com/css2?family=Poppins:wght@400;500;700&display=swap');
            body {
                font-family: 'Poppins', sans-serif;
                line-height: 1.6;
            }
        """
        # Add the new style tag to the head
        head.append(style_tag)
        # --- END FONT STYLING ---

        # Add the visible banner to the top of the body
        # Ensure a body tag exists for the banner to be inserted into
        body = soup.find('body')
        if body:
            banner_tag = soup.new_tag("div")
            banner_tag.string = "This content has been processed by the Juriscontent Generation Service."
            banner_tag.attrs['style'] = (
                "background-color: #F0F8FF; "
                "color: #333; "
                "text-align: center; "
                "padding: 8px; "
                "font-size: 14px;"
                "border-bottom: 1px solid #ADD8E6;"
            )
            body.insert(0, banner_tag)
            
        # Return the modified HTML as a string
        return str(soup)

