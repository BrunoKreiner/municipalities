import pandas as pd
import requests
import random
import queue
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from create_database import Municipality, Page, Link, Session  # Assuming you have a db.py file with these imports
import argparse
import logging

class WebCrawler:
    def __init__(self, csv_file):
        self.df = pd.read_csv(csv_file)
        self.session = Session()
        self.data_queue = queue.Queue()

    def fetch(self, url, depth=1):
        """
        Fetches the content of a webpage and its sublinks up to a specified depth
        """
        print(url)
        response = requests.get(url)
        page_content = response.content
        sublinks = []

        if depth > 1:
            soup = BeautifulSoup(page_content, 'html.parser')
            for link in soup.find_all('a'):
                sublink = link.get('href')
                if sublink and not sublink.startswith('http'):
                    sublink = urljoin(url, sublink)
                if sublink and sublink.startswith('http'):
                    sublinks.append(sublink)
        print({'url': url, 'content': page_content[0:100], 'sublinks': sublinks})
        return {'url': url, 'content': page_content, 'sublinks': sublinks}

    def fetch_random(self, num_samples, depth, num_workers):
        """
        Fetches the content of n randomly selected webpages and their sublinks up to a specified depth
        """
        logging.info(f"Fetching {num_samples} random samples with depth {depth} and {num_workers} workers.")
        random_rows = self.df.dropna(subset=['website']).sample(num_samples)
        print(self.df.columns)
        urls = random_rows.website.tolist()
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            results = executor.map(lambda url: self.fetch(url, depth), urls)
        for row, result in zip(random_rows.itertuples(), results):
            print(row)
            municipality = Municipality(
                name=row.municipality,
                country=row.country,
                email=row.email,
                population=row.population,
                pop_d=row.pop_d,
                status=row.status,
            )
            self.session.add(municipality)
            self.session.commit()  # commit here to get the id of the newly created municipality
            page = Page(
                url=result['url'],
                content=str(result['content']),
                municipality_id=municipality.id,
            )
            self.session.add(page)
            for sublink in result['sublinks']:
                subpage = Page(url=sublink)
                self.session.add(subpage)
                link = Link(
                    source_id=page.id,
                    destination_id=subpage.id,
                    depth=depth,
                    municipality_url=result['url'],
                )
                self.session.add(link)
            self.session.commit

def read_urls(file_path):
    with open(file_path, 'r') as f:
        urls = f.read().splitlines()

def main():
    parser = argparse.ArgumentParser(description="Run the web crawler")
    parser.add_argument("--num_samples", type=int, default=10, help="The number of samples to crawl")
    parser.add_argument("--depth", type=int, default=0, help="The number of depth to crawl sublinks, 0 means only the main page")
    parser.add_argument("--num_workers", type=int, default=10, help="The number of workers to use")
    args = parser.parse_args()

    crawler = WebCrawler(csv_file='../data/urls_processed.csv')

    results = crawler.fetch_random(num_samples=args.num_samples, depth=args.depth, num_workers=args.num_workers)

if __name__ == "__main__":
    main()