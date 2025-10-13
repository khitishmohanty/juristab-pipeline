from dataclasses import dataclass
from typing import Optional

@dataclass
class CaselawDocument:
    """Model for caselaw documents."""
    source_id: str
    book_type: str = "caselaw"
    book_name: str = ""
    neutral_citation: str = ""
    content: str = ""
    
    def to_dict(self):
        """Convert to dictionary for OpenSearch indexing."""
        return {
            "source_id": self.source_id,
            "book_type": self.book_type,
            "book_name": self.book_name,
            "neutral_citation": self.neutral_citation,
            "content": self.content
        }

@dataclass
class LegislationDocument:
    """Model for legislation documents."""
    source_id: str
    section_id: str
    book_type: str
    book_name: str
    section_name: str
    content: str
    
    def to_dict(self):
        """Convert to dictionary for OpenSearch indexing."""
        return {
            "source_id": self.source_id,
            "section_id": self.section_id,
            "book_type": self.book_type,
            "book_name": self.book_name,
            "section_name": self.section_name,
            "content": self.content
        }