# Section Extraction Feature

## Overview

This feature extends the text extraction service to fragment `juriscontent.html` files based on H1 headings and store each section as individual text files in S3.

## How It Works

1. **HTML Parsing**: The system reads `juriscontent.html` from S3
2. **Section Detection**: Identifies H1 headings as section boundaries
3. **Content Extraction**: Extracts content for each section, including:
   - Content before the first H1 (if any)
   - Each H1 heading and its associated content
   - Content between sections
4. **File Storage**: Saves each section as `miniviewer_n.txt` in S3
5. **Database Updates**: Records section information in the database

## File Structure

For a document with source_id `ABC123`, the files are organized as:

```
s3://bucket-name/
└── legislation/jurisdiction/
    └── ABC123/
        ├── miniviewer.html (original)
        ├── juriscontent.html (processed)
        └── section-level-content/
            ├── miniviewer_1.txt
            ├── miniviewer_2.txt
            ├── miniviewer_3.txt
            └── ...
```

## Section Numbering

- Sections are numbered sequentially starting from 1
- Each section gets a unique sequence number
- Content before the first H1 becomes section 1 (if present)
- Each H1 heading starts a new section
- No content is lost - all text is captured in sequential order

## Database Schema

### legislation_sections Table

Stores the mapping between source documents and their sections:

| Column | Type | Description |
|--------|------|-------------|
| id | char(36) | Primary key (UUID) |
| source_id | char(36) | Foreign key to source document |
| section_id | int | Section sequence number (1, 2, 3, ...) |

### legislation_enrichment_status Table

New status column added:

| Column | Type | Values |
|--------|------|--------|
| status_juriscontent_section_extract | enum | 'pass', 'failed', 'not started', 'started' |
| duration_juriscontent_section_extract | float | Processing duration in seconds |
| start_time_juriscontent_section_extract | datetime | Start timestamp |
| end_time_juriscontent_section_extract | datetime | End timestamp |

## Running the Service

### Mode Options

The service supports three running modes:

1. **Juriscontent Only**: Generates juriscontent.html files
   ```bash
   python main.py --mode juriscontent
   ```

2. **Sections Only**: Extracts sections from existing juriscontent.html files
   ```bash
   python main.py --mode sections
   ```

3. **Both** (default): Runs both stages sequentially
   ```bash
   python main.py --mode both
   # or simply
   python main.py
   ```

## Processing Logic

### Content Preservation

The extractor ensures NO content is lost:

- Text before the first H1 is preserved in section 1
- Each H1 and its content forms a complete section
- Text between H1s is captured in the appropriate section
- Text after the last H1 is included in the last section

### Section Content

Each section file contains:

1. The H1 heading text (if applicable)
2. All paragraphs, lists, and sub-headings (H2-H5) under that H1
3. Properly formatted text with paragraph breaks preserved

### Edge Cases

- **No H1 headings**: Entire content stored as single section (miniviewer_1.txt)
- **Empty sections**: Skipped, section numbers remain sequential
- **Nested content**: All sub-headings (H2-H5) are included in parent H1 section

## Configuration

Add to your `config.yaml`:

```yaml
tables:
  tables_to_write:
    - database: legal_store
      table: legislation_enrichment_status
      step_columns:
        text_extract:
          status: "status_juriscontent_html"
          duration: "duration_juriscontent_html"
          start_time: "start_time_juriscontent_html"
          end_time: "end_time_juriscontent_html"
        section_extract:
          status: "status_juriscontent_section_extract"
          duration: "duration_juriscontent_section_extract"
          start_time: "start_time_juriscontent_section_extract"
          end_time: "end_time_juriscontent_section_extract"
```

## Error Handling

- If section extraction fails, status is set to 'failed'
- Processing continues with next document
- Errors are logged with details
- Failed documents can be reprocessed

## Query Examples

### Find documents ready for section extraction
```sql
SELECT reg.source_id, reg.jurisdiction_code
FROM legislation_registry AS reg
INNER JOIN legislation_enrichment_status AS dest ON reg.source_id = dest.source_id
WHERE dest.status_juriscontent_html = 'pass'
  AND (dest.status_juriscontent_section_extract IS NULL 
       OR dest.status_juriscontent_section_extract != 'pass');
```

### Get all sections for a document
```sql
SELECT section_id
FROM legislation_sections
WHERE source_id = 'ABC123'
ORDER BY section_id;
```

### Check processing statistics
```sql
SELECT 
    status_juriscontent_section_extract,
    COUNT(*) as count,
    AVG(duration_juriscontent_section_extract) as avg_duration
FROM legislation_enrichment_status
WHERE status_juriscontent_section_extract IS NOT NULL
GROUP BY status_juriscontent_section_extract;
```

## Dependencies

No additional dependencies required beyond existing requirements.txt:
- beautifulsoup4 (already included)
- boto3 (already included)
- SQLAlchemy (already included)

## Monitoring

The service logs:
- Number of sections extracted per document
- Processing duration
- Success/failure status
- Detailed error messages for failures

Check audit logs:
```sql
SELECT * FROM audit_log 
WHERE job_name = 'legislation section extraction'
ORDER BY start_time DESC;
```