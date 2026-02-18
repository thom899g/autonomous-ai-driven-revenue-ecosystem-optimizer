import logging
from typing import Dict, Any
import requests
import json
from datetime import datetime
from pathlib import Path

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataCollector:
    """
    Collects data from various sources and stores it in a unified data lake.
    
    Attributes:
        data_sources (Dict[str, Dict]): Configuration for each data source.
        data_lake_path (Path): Path to the directory where data is stored.
    """

    def __init__(self):
        self.data_sources = {
            'crm': {'url': 'https://api.crm.example.com/revenue'},
            'finance': {'url': 'https://api.finance.example.com/financials'},
            'market': {'urls': ['https://api.market1.example.com', 'https://api.market2.example.com']}
        }
        self.data_lake_path = Path('data_lake').absolute()

    def fetch_data(self, source: str) -> Dict[str, Any]:
        """
        Fetches data from a specified source.
        
        Args:
            source (str): Name of the data source to fetch from.
            
        Returns:
            Dict[str, Any]: The fetched data along with metadata.
            
        Raises:
            requests.exceptions.RequestException: If fetching fails.
        """
        config = self.data_sources.get(source)
        if not config:
            raise ValueError(f"Data source '{source}' not configured.")
        
        try:
            start_time = datetime.now()
            logger.info(f"Starting data fetch from {source}.")
            
            if 'url' in config:
                response = requests.get(config['url'])
                response.raise_for_status()
                data = response.json()
            elif 'urls' in config:
                # Load balancing between URLs
                for url in config['urls']:
                    try:
                        response = requests.get(url)
                        response.raise_for_status()
                        data = response.json()
                        break
                    except (requests.exceptions.RequestException, json.JSONDecodeError):
                        logger.warning(f"Failed to fetch from {url}, trying next.")
                else:
                    raise RuntimeError("Could not fetch data from any URL in the list.")
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            # Store raw data
            source_path = self.data_lake_path / source
            source_path.mkdir(parents=True, exist_ok=True)
            timestamp = datetime.now().isoformat()
            file_path = source_path / f"{source}_{timestamp}.json"
            
            with open(file_path, 'w') as f:
                json.dump(data, f)
                
            logger.info(f"Data fetched from {source} successfully. Stored at {file_path}.")
            
            return {
                'status': 'success',
                'source': source,
                'timestamp': timestamp,
                'duration': duration
            }
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch data from {source}: {str(e)}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON response from {source}: {str(e)}")
            raise

    def collect_all_sources(self) -> Dict[str, Any]:
        """
        Fetches data from all configured sources and collects the results.
        
        Returns:
            Dict[str, Any]: Aggregated results of data collection.
        """
        results = []
        for source in self.data_sources.keys():
            try:
                result = self.fetch_data(source)
                results.append(result)
            except Exception as e:
                logger.error(f"Failed to collect data from {source}: {str(e)}")
                
        return {'status': 'completed', 'timestamp': datetime.now().isoformat(), 
                'results': results}

# Example usage
if __name__ == '__main__':
    collector = DataCollector()
    try:
        all_results = collector.collect_all_sources()
        logger.info("Data collection completed successfully.")
        print(json.dumps(all_results, indent=2))
    except Exception as e:
        logger.error(f"Critical error in data collection: {str(e)}")