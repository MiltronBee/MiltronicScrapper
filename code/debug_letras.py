#!/usr/bin/env python3
import logging
import requests
from bs4 import BeautifulSoup
import re
import time
from urllib.parse import urljoin

logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('debug_letras')

def fetch_and_analyze_artist_page(url):
    """Fetch and analyze the artist page to understand its structure"""
    logger.info(f"Fetching {url}")
    try:
        response = requests.get(url, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        response.raise_for_status()
        html_content = response.text
        logger.info(f"Got {len(html_content)} bytes from {url}")
        
        # Parse with BeautifulSoup
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Find all song list containers
        song_containers = soup.find_all('div', class_='songList-table')
        logger.info(f"Found {len(song_containers)} song containers")
        
        # Find all song rows
        song_rows = soup.find_all('li', class_=lambda x: x and 'songList-table-row' in x and '--song' in x)
        logger.info(f"Found {len(song_rows)} song rows")
        
        if song_rows:
            # Examine the first row's structure
            sample_row = song_rows[0]
            logger.info(f"Sample row HTML structure: {sample_row}")
            
            # Check for data attributes
            data_attrs = {}
            for attr in ['data-id', 'data-dns', 'data-url', 'data-artist', 'data-name', 'data-shareurl']:
                if attr in sample_row.attrs:
                    data_attrs[attr] = sample_row[attr]
            logger.info(f"Sample row data attributes: {data_attrs}")
            
            # Look for anchor tags regardless of class
            all_anchors = sample_row.find_all('a')
            logger.info(f"Found {len(all_anchors)} anchor tags in sample row")
            for i, anchor in enumerate(all_anchors):
                logger.info(f"Anchor {i+1} - href: {anchor.get('href')}, class: {anchor.get('class')}, text: {anchor.get_text().strip()}")
        
        # Extract songs
        songs = []
        for row in song_rows[:10]:  # Look at the first 10 for debugging
            # First try to get URL and name from data attributes
            artist_dns = row.get('data-dns', '')
            song_url_part = row.get('data-url', '')
            song_name = row.get('data-name', '')
            
            if artist_dns and song_url_part and song_name:
                full_url = f"https://www.letras.com/{artist_dns}/{song_url_part}/"
                songs.append({'title': song_name, 'url': full_url})
                logger.info(f"Found song from data attrs: {song_name} at {full_url}")
                continue
            
            # Fallback - look for the link element with class songList-table-songName
            link_elem = row.find('a', class_='songList-table-songName')
            if link_elem:
                href = link_elem.get('href', '')
                title = link_elem.get_text().strip()
                if href and title:
                    url = urljoin('https://www.letras.com', href)
                    songs.append({'title': title, 'url': url})
                    logger.info(f"Found song from anchor: {title} at {url}")
                    continue
            
            # If we're still here, try any anchor in the row
            for anchor in row.find_all('a'):
                href = anchor.get('href', '')
                title = anchor.get_text().strip()
                if href and title and ('/peso-pluma/' in href or '/junior-h/' in href):
                    url = urljoin('https://www.letras.com', href)
                    songs.append({'title': title, 'url': url})
                    logger.info(f"Found song from generic anchor: {title} at {url}")
                    break
        
        return songs

    except Exception as e:
        logger.error(f"Error analyzing {url}: {e}")
        return []

if __name__ == "__main__":
    # Test with a Peso Pluma
    songs = fetch_and_analyze_artist_page('https://www.letras.com/peso-pluma/')
    print(f"\nExtracted {len(songs)} songs from Peso Pluma page")
    
    # Try another artist to compare
    songs = fetch_and_analyze_artist_page('https://www.letras.com/junior-h/')
    print(f"\nExtracted {len(songs)} songs from Junior H page")
