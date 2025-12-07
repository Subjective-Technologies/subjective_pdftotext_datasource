"""
SubjectivePdfToTextDataSource - Subjective Technologies Data Source
Batch data source for converting PDF documents to JSON context files
"""

import os
import sys
import json
import logging
import hashlib
from typing import Dict, Any, List, Optional
from datetime import datetime
from PyPDF2 import PdfReader
from PyPDF2.errors import EmptyFileError
from subjective_abstract_data_source_package import SubjectiveDataSource


class SubjectivePdfToTextDataSource(SubjectiveDataSource):
    """
    SubjectivePdfToTextDataSource - Batch data source implementation
    
    This data source converts PDF documents to JSON format with metadata and extracted text.
    The JSON structure includes:
    - metadata (first node): name, data_type ("from_pdf"), timestamp, and PDF metadata
    - content: extracted text from all pages
    """
    
    def __init__(self, name=None, session=None, dependency_data_sources=[], subscribers=None, params=None):
        """
        Initialize the SubjectivePdfToTextDataSource.
        
        Args:
            name: Data source name
            session: Session object
            dependency_data_sources: List of dependency data sources
            subscribers: List of subscribers
            params: Parameters dictionary containing:
                - pdf_file_path: Path to the PDF file (required)
                - output_file_path: Path for output JSON file (optional, defaults to PDF name with .json extension)
                - include_page_numbers: Whether to include page numbers in text (default: True)
        """
        super().__init__(name, session, dependency_data_sources, subscribers, params)
        
        # Initialize logger
        self.logger = logging.getLogger(__name__)
        
        # Initialize configuration from params
        self.logger.info(f"Initializing with params: {self.params}")
        self.pdf_file_path = self.params.get('pdf_file_path', '')
        self.output_file_path = self.params.get('output_file_path', '')
        self.include_page_numbers = self.params.get('include_page_numbers', True)
        
        # Initialize progress tracking
        self._total_items = 0
        self._processed_items = 0
        self._total_processing_time = 0.0
        self._fetch_completed = False
        
        self.logger.info(f"Initialized SubjectivePdfToTextDataSource with PDF: '{self.pdf_file_path}'")
    
    def validate_config(self) -> bool:
        """
        Validate the configuration for this data source.
        
        Returns:
            bool: True if configuration is valid, False otherwise
        """
        self.logger.info(f"Validating config - pdf_file_path: '{self.pdf_file_path}'")
        
        if not self.pdf_file_path:
            self.logger.error("PDF file path is required")
            return False
        
        if not os.path.isfile(self.pdf_file_path):
            self.logger.error(f"PDF file does not exist: {self.pdf_file_path}")
            return False
        
        if not self.pdf_file_path.lower().endswith('.pdf'):
            self.logger.error(f"File is not a PDF: {self.pdf_file_path}")
            return False
        
        # Set default output path if not provided
        if not self.output_file_path:
            base_name = os.path.splitext(os.path.basename(self.pdf_file_path))[0]
            output_dir = os.path.dirname(self.pdf_file_path) or '.'
            self.output_file_path = os.path.join(output_dir, f"{base_name}.json")
            self.logger.info(f"Using default output path: {self.output_file_path}")
        
        self.logger.info("Configuration validation passed")
        return True
    
    def is_valid_pdf(self, pdf_path: str) -> bool:
        """Check if a PDF file is valid and not empty."""
        if not os.path.isfile(pdf_path) or os.path.getsize(pdf_path) == 0:
            return False
        try:
            PdfReader(pdf_path)
            return True
        except Exception:
            return False
    
    def compute_file_hash(self, file_path: str) -> str:
        """Compute SHA256 hash of a file."""
        sha256_hash = hashlib.sha256()
        try:
            with open(file_path, "rb") as f:
                for byte_block in iter(lambda: f.read(4096), b""):
                    sha256_hash.update(byte_block)
            return sha256_hash.hexdigest()
        except Exception as e:
            self.logger.error(f"Error computing file hash: {e}")
            return ""
    
    def extract_text_from_pdf(self, pdf_path: str) -> Dict[str, Any]:
        """
        Extract text from a PDF file.
        
        Args:
            pdf_path: Path to the PDF file
            
        Returns:
            Dict[str, Any]: Dictionary containing extracted text and metadata
        """
        extracted_data = {
            'pages': [],
            'full_text': '',
            'total_pages': 0,
            'total_characters': 0
        }
        
        try:
            reader = PdfReader(pdf_path)
            total_pages = len(reader.pages)
            extracted_data['total_pages'] = total_pages
            
            full_text_parts = []
            
            for page_num in range(total_pages):
                try:
                    page = reader.pages[page_num]
                    text = page.extract_text()
                    
                    if text:
                        page_data = {
                            'page_number': page_num + 1,
                            'text': text,
                            'character_count': len(text)
                        }
                        extracted_data['pages'].append(page_data)
                        full_text_parts.append(text)
                        
                        # Update progress
                        self.increment_processed_items()
                        
                except Exception as e:
                    self.logger.warning(f"Error processing page {page_num + 1} in {pdf_path}: {e}")
                    continue
            
            # Combine all text
            if self.include_page_numbers:
                # Add page separators with page numbers
                formatted_parts = []
                for page_data in extracted_data['pages']:
                    formatted_parts.append(f"\n--- Page {page_data['page_number']} ---\n")
                    formatted_parts.append(page_data['text'])
                extracted_data['full_text'] = '\n'.join(formatted_parts)
            else:
                extracted_data['full_text'] = '\n\n'.join(full_text_parts)
            
            extracted_data['total_characters'] = len(extracted_data['full_text'])
            
        except Exception as e:
            self.logger.error(f"Error reading PDF {pdf_path}: {e}")
            raise
        
        return extracted_data
    
    def create_json_structure(self, pdf_path: str, extracted_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create the JSON structure with metadata as first node.
        
        Args:
            pdf_path: Path to the PDF file
            extracted_data: Dictionary containing extracted text and page data
            
        Returns:
            Dict[str, Any]: Complete JSON structure
        """
        # Get file metadata
        file_stat = os.stat(pdf_path)
        file_name = os.path.basename(pdf_path)
        file_base_name = os.path.splitext(file_name)[0]
        file_size = file_stat.st_size
        file_hash = self.compute_file_hash(pdf_path)
        modified_time = datetime.fromtimestamp(file_stat.st_mtime).isoformat()
        current_timestamp = datetime.now().isoformat()
        
        # Create JSON structure with metadata as first node
        json_structure = {
            "metadata": {
                "name": file_base_name,
                "data_type": "from_pdf",
                "timestamp": current_timestamp,
                "pdf_file_name": file_name,
                "pdf_file_path": os.path.abspath(pdf_path),
                "pdf_file_size": file_size,
                "pdf_file_hash": file_hash,
                "pdf_modified_time": modified_time,
                "total_pages": extracted_data['total_pages'],
                "total_characters": extracted_data['total_characters'],
                "pages_with_text": len(extracted_data['pages']),
                "extraction_timestamp": current_timestamp
            },
            "content": {
                "full_text": extracted_data['full_text'],
                "pages": extracted_data['pages']
            }
        }
        
        return json_structure
    
    def fetch(self) -> List[Dict[str, Any]]:
        """
        Fetch data from the data source - convert PDF to JSON.
        
        Returns:
            List[Dict[str, Any]]: List containing the converted JSON data
        """
        try:
            if not self.validate_config():
                return []
            
            self.logger.info(f"Starting PDF to JSON conversion for: {self.pdf_file_path}")
            
            # Validate PDF file
            if not self.is_valid_pdf(self.pdf_file_path):
                self.logger.error(f"Invalid PDF file: {self.pdf_file_path}")
                return []
            
            # Set total items for progress tracking (total pages)
            reader = PdfReader(self.pdf_file_path)
            total_pages = len(reader.pages)
            self.set_total_items(total_pages)
            
            # Extract text from PDF
            start_time = datetime.now()
            extracted_data = self.extract_text_from_pdf(self.pdf_file_path)
            
            # Create JSON structure
            json_structure = self.create_json_structure(self.pdf_file_path, extracted_data)
            
            # Save to output file
            output_dir = os.path.dirname(self.output_file_path)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir, exist_ok=True)
            
            with open(self.output_file_path, 'w', encoding='utf-8') as f:
                json.dump(json_structure, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"Successfully converted PDF to JSON: {self.output_file_path}")
            self.set_total_processing_time((datetime.now() - start_time).total_seconds())
            self.set_fetch_completed(True)
            
            # Call progress callback if set
            if self.progress_callback:
                self.progress_callback(
                    self.get_name(),
                    self.get_total_to_process(),
                    self.get_total_processed(),
                    self.estimated_remaining_time()
                )
            
            # Call status callback if set
            if self.status_callback:
                self.status_callback(
                    self.get_name(),
                    f"Successfully converted PDF to JSON: {os.path.basename(self.output_file_path)}"
                )
            
            return [json_structure]
            
        except Exception as e:
            self.logger.error(f"Error in fetch: {e}")
            return []
    
    def get_icon(self) -> str:
        """
        Get the SVG icon content for this data source.
        
        Returns:
            str: SVG icon content if icon.svg exists, otherwise a fallback string
        """
        try:
            # Get the directory where this class file is located
            current_dir = os.path.dirname(os.path.abspath(__file__))
            icon_path = os.path.join(current_dir, "icon.svg")
            
            if os.path.exists(icon_path):
                with open(icon_path, 'r', encoding='utf-8') as f:
                    return f.read()
            else:
                # Fallback SVG icon for PDF to text
                return '''<?xml version="1.0" encoding="UTF-8"?>
<svg width="64" height="64" viewBox="0 0 64 64" xmlns="http://www.w3.org/2000/svg">
  <defs>
    <linearGradient id="gradient" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" style="stop-color:#4ECDC4;stop-opacity:1" />
      <stop offset="100%" style="stop-color:#4ECDC480;stop-opacity:1" />
    </linearGradient>
  </defs>
  
  <!-- Background circle -->
  <circle cx="32" cy="32" r="30" fill="url(#gradient)" stroke="#333" stroke-width="2"/>
  
  <!-- PDF icon symbol -->
  <text x="32" y="42" font-family="Arial, sans-serif" font-size="24" text-anchor="middle" fill="white">
    📄
  </text>
  
  <!-- Data source type indicator -->
  <text x="32" y="56" font-family="Arial, sans-serif" font-size="8" text-anchor="middle" fill="#333">
    PDF→JSON
  </text>
  
  <!-- Corner indicator for Subjective Technologies -->
  <circle cx="52" cy="12" r="8" fill="#333" opacity="0.8"/>
  <text x="52" y="16" font-family="Arial, sans-serif" font-size="8" text-anchor="middle" fill="white">S</text>
</svg>'''
                
        except Exception as e:
            self.logger.error(f"Error reading icon file: {e}")
            # Return a simple fallback string
            return "📄 PDF to Text Data Source"
    
    def get_connection_data(self) -> Dict[str, Any]:
        """
        Return the connection type and required fields for this data source.
        
        Returns:
            Dict[str, Any]: Connection data dictionary
        """
        return {
            "connection_type": "FileSystem",
            "fields": {
                "pdf_file_path": {
                    "type": "string",
                    "required": True,
                    "description": "Path to the PDF file to convert"
                },
                "output_file_path": {
                    "type": "string", 
                    "required": False,
                    "description": "Path for output JSON file (defaults to PDF name with .json extension)"
                },
                "include_page_numbers": {
                    "type": "bool",
                    "required": False,
                    "default": True,
                    "description": "Whether to include page numbers in extracted text"
                }
            }
        }
    
    def get_connection_metadata(self) -> Dict[str, Any]:
        """
        Get connection metadata for interactive parameter collection.
        
        Returns:
            Dict[str, Any]: Dictionary describing required connection parameters
        """
        return {
            'pdf_file_path': {
                'description': 'Path to the PDF file to convert to JSON',
                'type': 'string',
                'required': True,
                'default': '',
                'sensitive': False
            },
            'output_file_path': {
                'description': 'Path for output JSON file (optional, defaults to PDF name with .json extension)',
                'type': 'string',
                'required': False,
                'default': '',
                'sensitive': False
            },
            'include_page_numbers': {
                'description': 'Whether to include page numbers in extracted text',
                'type': 'bool',
                'required': False,
                'default': True,
                'sensitive': False
            }
        }


def main():
    """
    Main entry point for running the SubjectivePdfToTextDataSource directly.
    """
    from dotenv import load_dotenv
    
    # Load environment variables
    load_dotenv()
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Configuration
    params = {
        'pdf_file_path': os.getenv('PDF_FILE_PATH', ''),
        'output_file_path': os.getenv('OUTPUT_FILE_PATH', ''),
        'include_page_numbers': os.getenv('INCLUDE_PAGE_NUMBERS', 'true').lower() == 'true'
    }
    
    # Create and run data source
    datasource = SubjectivePdfToTextDataSource(params=params)
    
    try:
        # Fetch and convert PDF to JSON
        results = datasource.fetch()
        
        if results:
            json_data = results[0]
            metadata = json_data.get('metadata', {})
            
            print(f"✅ PDF to JSON conversion completed successfully!")
            print(f"📄 PDF File: {metadata.get('pdf_file_name', 'Unknown')}")
            print(f"📊 Total Pages: {metadata.get('total_pages', 0)}")
            print(f"📝 Total Characters: {metadata.get('total_characters', 0):,}")
            print(f"💾 Output saved to: {datasource.output_file_path}")
            print(f"🏷️  Data Type: {metadata.get('data_type', 'Unknown')}")
            print(f"⏰ Timestamp: {metadata.get('timestamp', 'Unknown')}")
        else:
            print("❌ Conversion failed or no data extracted")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

