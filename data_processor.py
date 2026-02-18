import logging
from typing import Dict, Any, Optional
import json
from pathlib import Path

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataProcessor:
    """
    Processes raw data from the data lake and prepares it for analysis.
    
    Attributes:
        data_lake_path (Path): Path to the directory containing raw data.
        processed_data_path (Path): Path to the directory where processed data is stored.
    """

    def __init__(self):
        self.data_lake_path = Path('data_lake').absolute()
        self.processed_data_path = Path('processed_data').absolute()

    def process_source(self, source: str) -> Dict[str, Any]:
        """
        Processes all data from a specific source and stores it in the processed directory.
        
        Args:
            source (str): Name of the data source to process.
            
        Returns:
            Dict[str, Any]: Metadata about the processing operation.
            
        Raises:
            FileNotFoundError: If no data found for the source.
        """
        start_time = datetime.now()
        logger.info(f"Starting data processing from {source}.")
        
        # Find all JSON files in the source directory
        source_dir = self.data_lake_path / source
        if not source_dir.exists():
            raise FileNotFoundError(f"No data found for source {source}.")
            
        processed_data = []
        for file in source_dir.glob('*.json'):
            try:
                with open(file, 'r') as f:
                    data = json.load(f)
                    
                # Example processing: filter out negative revenue values
                if isinstance(data, list):
                    filtered = [item for item in data if item.get('revenue', 0) > 0]
                else:
                    filtered = data.copy()
                    if 'revenue' in filtered and filtered['revenue'] <= 0:
                        continue
                
                processed_data.append(filtered)
                
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse JSON file {file}: {str(e)}")
            
        # Store processed data