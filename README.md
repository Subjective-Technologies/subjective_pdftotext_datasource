# SubjectivePdfToTextDataSource

Batch data source for converting PDF documents to JSON context files - Part of Subjective Technologies Data Source ecosystem.

## Overview

This data source converts PDF documents into JSON format with structured metadata and extracted text content. The JSON structure includes metadata as the first node with the data type name "from_pdf", timestamp, and comprehensive PDF information.

## Features

- 📄 **PDF Text Extraction**: Extracts text from all pages of a PDF document
- 📊 **Structured JSON Output**: Creates well-structured JSON with metadata and content
- 🏷️ **Metadata First**: Metadata node is the first element with data_type "from_pdf"
- ⏰ **Timestamp Tracking**: Includes extraction timestamp and PDF modification time
- 📝 **Page-Level Extraction**: Extracts text with optional page number markers
- 🔍 **File Validation**: Validates PDF files before processing
- 💾 **Automatic Output**: Generates output JSON file automatically
- 🛡️ **Error Handling**: Robust error handling for corrupted or invalid PDFs
- 📈 **Progress Tracking**: Real-time progress updates during extraction

## Installation

### Using Pip

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

## Configuration

Create a `.env` file in the project root with the following variables:

```env
# PDF Configuration
PDF_FILE_PATH=./path/to/document.pdf
OUTPUT_FILE_PATH=./output/document.json  # Optional, defaults to PDF name with .json extension
INCLUDE_PAGE_NUMBERS=true  # Whether to include page numbers in text
```

## Usage

### Direct Execution

```bash
python SubjectivePdfToTextDataSource.py
```

### Programmatic Usage

```python
from SubjectivePdfToTextDataSource import SubjectivePdfToTextDataSource

# Configuration
params = {
    'pdf_file_path': './path/to/document.pdf',
    'output_file_path': './output/document.json',  # Optional
    'include_page_numbers': True  # Optional, default: True
}

# Create and run data source
datasource = SubjectivePdfToTextDataSource(params=params)

# Convert PDF to JSON
results = datasource.fetch()

# Access the JSON structure
if results:
    json_data = results[0]
    metadata = json_data['metadata']
    content = json_data['content']
    
    print(f"Data Type: {metadata['data_type']}")
    print(f"Name: {metadata['name']}")
    print(f"Timestamp: {metadata['timestamp']}")
    print(f"Total Pages: {metadata['total_pages']}")
    print(f"Full Text Length: {len(content['full_text'])} characters")
```

### Using subcli

```bash
# Execute with interactive parameter collection
subcli --execute-data-source=pdftotext --collect-metadata
```

## JSON Structure

The data source creates JSON files with the following structure:

```json
{
  "metadata": {
    "name": "document",
    "data_type": "from_pdf",
    "timestamp": "2024-01-01T12:00:00.000000",
    "pdf_file_name": "document.pdf",
    "pdf_file_path": "/absolute/path/to/document.pdf",
    "pdf_file_size": 1024000,
    "pdf_file_hash": "a1b2c3d4e5f6...",
    "pdf_modified_time": "2024-01-01T10:00:00.000000",
    "total_pages": 10,
    "total_characters": 50000,
    "pages_with_text": 10,
    "extraction_timestamp": "2024-01-01T12:00:00.000000"
  },
  "content": {
    "full_text": "--- Page 1 ---\nExtracted text from page 1...\n\n--- Page 2 ---\n...",
    "pages": [
      {
        "page_number": 1,
        "text": "Extracted text from page 1...",
        "character_count": 2500
      },
      {
        "page_number": 2,
        "text": "Extracted text from page 2...",
        "character_count": 2300
      }
    ]
  }
}
```

## Output Structure

The data source creates the following output:

```
./output/
└── document.json  # JSON file with metadata and extracted content
```

## Configuration Options

### Required Parameters

- **pdf_file_path** (required): Path to the PDF file to convert

### Optional Parameters

- **output_file_path** (optional): Path for output JSON file. If not provided, defaults to the PDF file name with `.json` extension in the same directory
- **include_page_numbers** (optional): Whether to include page number markers in the full text (default: true)

### Processing Behavior

- **Text Extraction**: Uses PyPDF2 for reliable text extraction
- **Page Processing**: Processes all pages sequentially
- **Error Handling**: Skips invalid or corrupted pages, continues processing
- **Progress Tracking**: Shows real-time progress during extraction
- **File Validation**: Validates PDF file before processing

## Development

### Adding Custom Logic

1. **Text Processing**: Modify the `extract_text_from_pdf()` method for custom text processing
2. **Metadata**: Customize metadata fields in the `create_json_structure()` method
3. **Output Format**: Modify the JSON structure in `create_json_structure()` method

### Testing

```bash
# Create test PDF file or use existing one
# Run the data source
python SubjectivePdfToTextDataSource.py

# Check output
cat output/document.json
```

### Debug Mode

```bash
# Run with debug logging
export LOG_LEVEL=DEBUG
python SubjectivePdfToTextDataSource.py
```

## Troubleshooting

### Common Issues

1. **No Text Extracted**: Some PDFs may be image-based and not contain extractable text
2. **Invalid PDFs**: Check for corrupted or empty PDF files
3. **Permission Errors**: Ensure read permissions on the PDF file and write permissions on the output directory
4. **Memory Issues**: Very large PDFs may require more memory

### Performance Tips

1. **Large PDFs**: The system processes pages sequentially for memory efficiency
2. **Batch Processing**: Process multiple PDFs by calling the data source multiple times
3. **Output Location**: Use fast storage for output files

### Logging

The data source provides comprehensive logging. Set the `LOG_LEVEL` environment variable:

```bash
export LOG_LEVEL=DEBUG  # DEBUG, INFO, WARNING, ERROR
```

## Example Output

After processing a PDF, you'll get a JSON file like:

```json
{
  "metadata": {
    "name": "research_paper",
    "data_type": "from_pdf",
    "timestamp": "2024-01-15T14:30:00.123456",
    "pdf_file_name": "research_paper.pdf",
    "pdf_file_path": "/home/user/documents/research_paper.pdf",
    "pdf_file_size": 2048576,
    "pdf_file_hash": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    "pdf_modified_time": "2024-01-10T09:15:00.000000",
    "total_pages": 25,
    "total_characters": 125000,
    "pages_with_text": 25,
    "extraction_timestamp": "2024-01-15T14:30:00.123456"
  },
  "content": {
    "full_text": "--- Page 1 ---\nAbstract\nThis paper presents...\n\n--- Page 2 ---\nIntroduction\n...",
    "pages": [...]
  }
}
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is part of Subjective Technologies and follows the organization's licensing terms.

## Support

For support and questions:
- Create an issue in the repository
- Contact Subjective Technologies support
- Check the documentation at [Subjective Technologies](https://github.com/Subjective-Technologies)

---

Generated by Subjective CLI (subcli) v1.0.0

