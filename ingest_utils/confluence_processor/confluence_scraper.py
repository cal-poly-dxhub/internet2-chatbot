import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import os
from typing import List, Dict, Optional

class ConfluenceScraper:
    def __init__(self, base_url: str):
        self.base_url = base_url

    def _get_file_type(self, url: str) -> str:
        """
        Determines the file type from a URL
        """
        parsed_url = urlparse(url)
        path = parsed_url.path
        _, ext = os.path.splitext(path)
        return ext.lstrip('.').lower()

    def scrape_assets(self) -> List[Dict]:
        """
        Scrapes the Confluence page for assets, applying the MP4 file rule
        Returns a list of dictionaries, each containing asset info
        """
        try:
            response = requests.get(self.base_url)
            response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)
        except requests.exceptions.RequestException as e:
            print(f"Error fetching page {self.base_url}: {e}")
            return []

        soup = BeautifulSoup(response.content, 'html.parser')
        assets_data = []

        # Find the table containing the calendar data
    
        tables = soup.find_all('table')
        target_table = None
        for table in tables:
            if table.find('th', string='Assets'):
                target_table = table
                break
        
        if not target_table:
            print("Could not find the target table on the page.")
            return []

        # Iterate through rows in the table body
        # Skip the header rows (usually in <thead>)
        for row in target_table.find('tbody').find_all('tr'):
            cols = row.find_all('td')
            if len(cols) < 5:  # Ensure it's a data row, not a header or empty row
                continue
            
            assets_col = cols[4] # 'Assets' column is the 5th column (index 4)

            has_mp4_in_block = False
            current_block_assets = []

            # First pass to identify MP4s in the block
            for a_tag in assets_col.find_all('a', href=True):
                link_url = urljoin(self.base_url, a_tag['href'])
                file_type = self._get_file_type(link_url)
                if file_type == 'mp4':
                    has_mp4_in_block = True
                    break
            
            # Second pass to collect assets based on rules
            # Collect all potential asset tags (a and img)
            asset_tags = []
            asset_tags.extend(assets_col.find_all('a', href=True))
            asset_tags.extend(assets_col.find_all('img', src=True))

            for tag in asset_tags:
                link_url = ""
                if tag.name == 'a':
                    link_url = urljoin(self.base_url, tag['href'])
                elif tag.name == 'img':
                    link_url = urljoin(self.base_url, tag['src'])

                if not link_url: # Skip if no valid URL found
                    continue

                file_type = self._get_file_type(link_url)
                
                is_subscriber_content = "(subscribers)" in assets_col.get_text(strip=True).lower()

                if file_type == 'mp4':
                    current_block_assets.append({
                        'url': link_url,
                        'file_type': file_type,
                        'is_subscriber_content': is_subscriber_content
                    })
                elif has_mp4_in_block and file_type in ['vtt', 'm4a']:
                    # Skip VTT and M4A if MP4 is present in the same block
                    continue
                else:
                    # Always include other file types, and VTT/M4A if no MP4 in block
                    # and ensure they are not empty links which are often placeholders.
                    if link_url and not link_url.endswith('/') and 'download/thumbnails' not in link_url: # Exclude folder links, empty URLs, and Confluence thumbnails
                        current_block_assets.append({
                            'url': link_url,
                            'file_type': file_type,
                            'is_subscriber_content': is_subscriber_content
                        })
            assets_data.extend(current_block_assets)

        return assets_data 