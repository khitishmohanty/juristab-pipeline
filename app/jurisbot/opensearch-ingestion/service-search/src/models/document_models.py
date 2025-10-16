from dataclasses import dataclass
from typing import Optional, List
from datetime import datetime
import json
import re

def parse_date_to_iso(date_string: Optional[str]) -> Optional[str]:
    """Parse various date formats to ISO 8601 format for OpenSearch."""
    if not date_string:
        return None
    
    date_string = str(date_string).strip()
    
    # Skip non-date values
    skip_patterns = [
        'no date', 'not provided', 'unknown', 'n/a', 'nil', 'none', 
        'not available', 'not specified', 'tba', 'tbd'
    ]
    if any(pattern in date_string.lower() for pattern in skip_patterns):
        return None
    
    # Common date formats to try
    date_formats = [
        '%d %B %Y',           # 21 December 2018
        '%d %b %Y',           # 21 Dec 2018
        '%B %d, %Y',          # December 21, 2018
        '%b %d, %Y',          # Dec 21, 2018
        '%d/%m/%Y',           # 21/12/2018
        '%m/%d/%Y',           # 12/21/2018
        '%Y-%m-%d',           # 2018-12-21
        '%Y/%m/%d',           # 2018/12/21
        '%d-%m-%Y',           # 21-12-2018
        '%d-%b-%Y',           # 21-Dec-2018
        '%d %B %Y %H:%M:%S',  # 21 December 2018 14:30:00
        '%Y-%m-%d %H:%M:%S',  # 2018-12-21 14:30:00
        '%Y-%m-%dT%H:%M:%S',  # 2018-12-21T14:30:00
        '%Y-%m-%dT%H:%M:%SZ', # 2018-12-21T14:30:00Z
    ]
    
    for fmt in date_formats:
        try:
            dt = datetime.strptime(date_string, fmt)
            return dt.strftime('%Y-%m-%d')
        except ValueError:
            continue
    
    return None

@dataclass
class CaselawDocument:
    """Model for caselaw documents with ONLY requested metadata fields."""
    # Required fields
    source_id: str
    book_name: str  # From caselaw_registry
    neutral_citation: str
    content: str
    
    # ONLY the requested metadata fields from caselaw_metadata table
    file_no: Optional[str] = None
    presiding_officer: Optional[str] = None
    counsel: Optional[str] = None
    law_firm_agency: Optional[str] = None
    court_type: Optional[str] = None
    hearing_location: Optional[str] = None
    keywords: Optional[str] = None
    legislation_cited: Optional[str] = None
    affected_sectors: Optional[str] = None
    practice_areas: Optional[str] = None
    citation: Optional[str] = None
    key_issues: Optional[str] = None
    panelist: Optional[str] = None
    cases_cited: Optional[str] = None
    matter_type: Optional[str] = None
    category: Optional[str] = None
    bjs_number: Optional[str] = None
    tribunal_name: Optional[str] = None
    panel_or_division_name: Optional[str] = None
    year: Optional[int] = None
    decision_number: Optional[int] = None
    decision_date: Optional[str] = None
    members: Optional[str] = None
    
    def to_dict(self):
        """Convert to dictionary for OpenSearch indexing using DB field names directly."""
        # Start with required fields
        doc = {
            "source_id": self.source_id,
            "document_type": "caselaw",
            "book_name": self.book_name,  # Using DB field name directly
            "neutral_citation": self.neutral_citation,
            "content": self.content,
            "indexed_date": datetime.now().isoformat(),
            "content_length": len(self.content) if self.content else 0
        }
        
        # Add ONLY the requested fields using DB field names directly
        
        if self.file_no:
            doc["file_no"] = self.file_no
            
        if self.presiding_officer:
            doc["presiding_officer"] = self.presiding_officer
            
        if self.counsel:
            doc["counsel"] = self.counsel
            
        if self.law_firm_agency:
            doc["law_firm_agency"] = self.law_firm_agency
            
        if self.court_type:
            doc["court_type"] = self.court_type
            
        if self.hearing_location:
            doc["hearing_location"] = self.hearing_location
            
        if self.keywords:
            doc["keywords"] = self.keywords
            
        if self.legislation_cited:
            doc["legislation_cited"] = self.legislation_cited
            
        if self.affected_sectors:
            # Split by comma for array field
            doc["affected_sectors"] = [sector.strip() for sector in self.affected_sectors.split(',') if sector.strip()]
            
        if self.practice_areas:
            # Split by comma for array field
            doc["practice_areas"] = [area.strip() for area in self.practice_areas.split(',') if area.strip()]
            
        if self.citation:
            doc["citation"] = self.citation
            
        if self.key_issues:
            doc["key_issues"] = self.key_issues
            
        if self.panelist:
            doc["panelist"] = self.panelist
            
        if self.cases_cited:
            doc["cases_cited"] = self.cases_cited
            
        if self.matter_type:
            doc["matter_type"] = self.matter_type
            
        if self.category:
            doc["category"] = self.category
            
        if self.bjs_number:
            doc["bjs_number"] = self.bjs_number
            
        if self.tribunal_name:
            doc["tribunal_name"] = self.tribunal_name
            
        if self.panel_or_division_name:
            doc["panel_or_division_name"] = self.panel_or_division_name
            
        if self.year:
            doc["year"] = self.year
            
        if self.decision_number:
            doc["decision_number"] = self.decision_number
            
        if self.decision_date:
            # Parse date to ISO format
            parsed_date = parse_date_to_iso(self.decision_date)
            if parsed_date:
                doc["decision_date"] = parsed_date
            
        if self.members:
            doc["members"] = self.members
            
        return doc

@dataclass
class LegislationDocument:
    """Model for legislation documents with ONLY requested metadata fields."""
    # Required fields
    source_id: str
    section_id: str
    book_name: str  # From legislation_registry
    content: str
    
    # Section name is now optional and will always be blank
    section_name: str = ''
    
    # ONLY the requested metadata fields from legislation_metadata table
    legislation_number: Optional[str] = None
    type_of_document: Optional[str] = None
    enabling_act: Optional[str] = None
    amended_legislation: Optional[str] = None
    administering_agency: Optional[str] = None
    affected_sectors: Optional[str] = None
    practice_areas: Optional[str] = None
    keywords: Optional[str] = None
    
    def to_dict(self):
        """Convert to dictionary for OpenSearch indexing using DB field names directly."""
        # Start with required fields
        doc = {
            "source_id": self.source_id,
            "section_id": self.section_id,
            "document_type": self.type_of_document if self.type_of_document else "legislation",  # From legislation_metadata
            "book_name": self.book_name,  # From legislation_registry
            "section_name": '',  # Always blank as requested
            "content": self.content,
            "indexed_date": datetime.now().isoformat(),
            "content_length": len(self.content) if self.content else 0
        }
        
        # Add ONLY the requested fields using DB field names directly
        
        if self.legislation_number:
            doc["legislation_number"] = self.legislation_number
            
        if self.type_of_document:
            doc["type_of_document"] = self.type_of_document  # Store separately as well
            
        if self.enabling_act:
            doc["enabling_act"] = self.enabling_act
            
        if self.amended_legislation:
            doc["amended_legislation"] = self.amended_legislation
            
        if self.administering_agency:
            doc["administering_agency"] = self.administering_agency
            
        if self.affected_sectors:
            # Split by comma for array field
            doc["affected_sectors"] = [sector.strip() for sector in self.affected_sectors.split(',') if sector.strip()]
            
        if self.practice_areas:
            # Split by comma for array field
            doc["practice_areas"] = [area.strip() for area in self.practice_areas.split(',') if area.strip()]
            
        if self.keywords:
            doc["keywords"] = self.keywords
            
        return doc