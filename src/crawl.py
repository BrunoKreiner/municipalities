import asyncio
import aiohttp
from bs4 import BeautifulSoup
import pandas as pd
from urllib.parse import urljoin
from create_database import Municipality, Page 
import argparse
from urllib.parse import urljoin, urlparse, urlsplit
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from aiohttp import ClientSession, ClientTimeout
from tqdm import tqdm
import math
import json
timeout = ClientTimeout(total=10)
import logging
async def on_request_start(session, context, params):
    logging.getLogger('aiohttp.client').debug(f'Starting request <{params}>')


async def crawl_url(client, url):
    """Crawls a specific url given a client and the url.

    Args:
        client (aiohttp.ClientSession): The client to use for the request
        url (str): The URL to crawl 
        timeout (float, optional): The timeout in seconds. Defaults to 15.0.

    Returns:
        aiohttp.client_reqrep.ClientResponse: The response from the request OR a string with the error message if the request failed
    """
    response = None
    try:
        response = await asyncio.wait_for(client.get(url, ssl=False), timeout=15.0)
    except asyncio.TimeoutError as e:
        print(f"Municipality Timeout occurred while trying to connect to {url}")
        return e
    except aiohttp.ClientConnectorError as e:
        print(f"ClientConnectorError occurred while trying to connect to {url}: {e}")
        # If the connection failed, try using HTTPS instead of HTTP
        try:
            if 'http://' in url:
                url = url.replace('http://', 'https://')
            print(f"Retrying with SSL = True: {url}")
            response = await asyncio.wait_for(client.get(url, ssl=True), timeout=15.0)
        except Exception as e:
            print(f"Unhandled exception occurred while trying to connect to {url}: {e} after trying SSL = True and changing to https")
            return e
    except Exception as e:
        error_message = f"Unhandled exception occurred while trying to connect to {url}: {e} after trying to get the response"
        print(error_message.encode('utf-8', 'surrogateescape').decode('utf-8'))
        return e
    return response


def add_http(url):
    if not url.startswith(('http://', 'https://')):
        url = 'http://' + url
    return url

async def fetch_subdata(base_url, tags, tag_name):
    """fetches the subdata (css, javascript) from a base url based on the tags and tag_name ("href" for css, "ref" for javascript)

    Args:
        base_url (str): base url of the website to fetch more data from
        tags (list[]): list of tags
        tag_name (_type_): tag_name to fetch from the tag ("href" for css, "ref" for javascript)

    Returns:
        dict: Dictionary with keys being the url and the value being the content of the url
    """
    subdata = {}
    for tag in tags:
        js_url = tag[tag_name]

        # If the URL is not absolute, we need to make it absolute
        if not js_url.startswith('http'):
            base_url = "{0.scheme}://{0.netloc}".format(urlsplit(base_url))
            js_url = urljoin(base_url, js_url)

        async with ClientSession(timeout = timeout, trust_env = True) as client:
            js_content = await crawl_url(client, js_url)
            if js_content is None:
                subdata[js_url] = 'None'
            else:
                try:
                    subdata[js_url] = await js_content.text()
                except Exception as e:
                    subdata[js_url] = str(e)
    return subdata

class WebCrawler:
    def __init__(self, csv_file, depth= 0):
        """Initializes the WebCrawler.

        Args:
            csv_file (str): Path to the csv file containing the municipalities
            depth (int, optional): Depth to crawl sublinks. Defaults to 0 meaning no sublinks are crawled.
        """
        self.depth = depth
        self.df = pd.read_csv(csv_file)
        self.engine = create_engine('sqlite:///../data/municipalities.db')
        self.Session = scoped_session(sessionmaker(bind=self.engine))
        self.data_queue = asyncio.Queue()
        self.done = False  # Flag to indicate when no more items will be added to the queue
        self.pbar_municipalities = None
        self.pbar_sublinks = None


    def add_municipality(self, data):
        session = self.Session()
        municipality = Municipality(
            id = data["municipality_id"],
            name=data["municipality_name"],
            country=data["country"],
            email=data["email"],
            population=data["population"],
            pop_d=data["pop_d"],
            url = data["url"],
            content = data["page_content"],
            status=data["status"],
            scripts=data["scripts"],
            sublinks_count = data["sublinks_count"],
            css_files=data["css_files"]
        )
        session.add(municipality)
        session.commit()

    async def update_progress(self, pbar):
        """Updates the progress bar of a tqdm progress bar

        Args:
            pbar (tqdm obj): Progress bar to update
        """
        while not self.done or not self.data_queue.empty():
            pbar.total = self.data_queue.qsize() + pbar.n
            pbar.refresh()
            await asyncio.sleep(0.1)  # adjust this to control how often the progress bar updates
        pbar.close()

    async def fetch_municipality(self, row):
        """
        Fetches the content of a webpage and its sublinks up to a specified depth

        Args:
            row (pd.DatFrame row): row with municipality url
        """
        try:
            i, row = row
        except Exception as e:
            print(f"Failed to unpack row due to {str(e)}")
            print("row object: ", row)
            return None

        current_depth = 0
        municipality_id = row["index"]
        url = row.website
        url = add_http(url)
        scripts_content = []
        css_content = []
        a_list = []

        async with ClientSession(timeout = timeout, trust_env = True) as client:
                page_content = await crawl_url(client, url)
                # check if response is of type Response:
                if isinstance(page_content, aiohttp.client_reqrep.ClientResponse):
                    try:
                        page_content = await page_content.text()
                    except Exception as e:
                        print(f"Cannot decode content from URL {url} as text. Skipping.")
                        return

                    soup = BeautifulSoup(page_content, 'html.parser')
                    #script_tags = soup.find_all('script', attrs={'src': True})
                    #css_tags = soup.find_all('link', attrs={'href': True, 'rel': 'stylesheet'})

                    #scripts_content = await fetch_subdata(url, script_tags, 'src')
                    #css_content = await fetch_subdata(url, css_tags, 'href')
                    a_list = soup.find_all('a', href=True)
                    if self.depth > 0:
                        for a in a_list:
                            sublink = a['href']
                            parsed = urlparse(sublink)
                            # What to do with external links?
                            if parsed.scheme and parsed.netloc and parsed.netloc != urlparse(url).netloc:
                                continue  # Skip if this link goes to an external site
                            if sublink.startswith('#'):
                                continue  # Skip if this is a fragment identifier
                            sublink = urljoin(url, sublink)  # Handle relative links
                            sublink_obj = {'source_page_id': None, 'source_url': url, "municipality_id": municipality_id, 'sublink-url': sublink, 'depth': current_depth + 1}
                            await self.data_queue.put(sublink_obj)  # Add the sublink to the queue to fetch content
                await client.close()

        self.add_municipality({
            "municipality_id": municipality_id,
            "municipality_name": row.municipality,
            "country": row.country,
            "email": row.email,
            "population": row.population,
            "pop_d": row.pop_d,
            "url": url,
            "page_content": str(page_content),
            "status": row.status,
            "scripts": json.dumps(scripts_content),
            "sublinks_count": len(a_list),
            "css_files": json.dumps(css_content)
        })
        self.pbar_municipalities.update(1)
        return {'url': url, 'content': page_content}

    async def process_sublink(self):
        """
        Fetches the content of a sublink and adds it to the database.
        If depth > 0 and the sublink is not at the maximum depth, it will also add the sublinks of the sublink to the queue.
        """
        while True:
            try:
                session = self.Session()
                # Try to get a sublink from the queuetry:
                try:
                    sublink_obj = await asyncio.wait_for(self.data_queue.get(), timeout=15.0)
                except asyncio.TimeoutError:
                    print("Queue is empty")
                    break
                except Exception as e:
                    print(f"Unhandled exception occurred while trying to get a sublink from the queue: {e}")
                    continue

                #print(f"Found sublink; {sublink_obj['sublink-url']}, depth: {sublink_obj['depth']}")
                url = sublink_obj["sublink-url"]
                url = add_http(url)
                async with ClientSession(timeout = timeout, trust_env = True) as client:
                    page_content = await crawl_url(client, url)
                    if page_content is None:
                        continue
                    # check if response is of type Response:
                    if isinstance(page_content, aiohttp.client_reqrep.ClientResponse):
                        try:
                            page_content = await page_content.text()
                        except UnicodeDecodeError as e:
                            #print(f"Cannot decode content from URL {url} as text. Storing as 'not_downloaded'.")
                            page_content = 'not_downloaded'

                # Add the sublink to the database
                try:
                    subpage = Page(
                        url=sublink_obj['sublink-url'], 
                        content=str(page_content), 
                        depth = sublink_obj["depth"],
                        municipality_id = sublink_obj["municipality_id"],
                        source_id = sublink_obj["source_page_id"]
                    )
                    session.add(subpage)
                    session.commit()

                    if self.depth > 0 and sublink_obj["depth"] < self.depth:
                        soup = BeautifulSoup(page_content, 'html.parser')
                        for a in soup.find_all('a', href=True):
                            new_sublink = a['href']
                            
                            parsed = urlparse(new_sublink)

                            if parsed.scheme and parsed.netloc and parsed.netloc != urlparse(url).netloc:
                                continue  # Skip if this link goes to an external site
                            if new_sublink.startswith('#'):
                                continue  # Skip if this is a fragment identifier

                            new_sublink = urljoin(url, new_sublink)  # Handle relative links
                            new_sublink_obj = {'source_page_id': subpage.id, 'source_url': url, "municipality_id": sublink_obj["municipality_id"], 'sublink-url': new_sublink, 'depth': sublink_obj["depth"] + 1}
                            await self.data_queue.put(new_sublink_obj)
                                            
                except Exception as e:
                    print(f"Failed to add page and link to the database due to {str(e)}")
                    session.rollback()
                finally:
                    self.pbar_sublinks.update(1)
                    session.close() 

            except asyncio.QueueEmpty:
                if self.done:
                    break  # Exit the loop if no more items will be added to the queue
        print("Closing session")
        session.close() # Exit the loop if no more items will be added to the queue

    async def fetch_random(self, num_samples, num_workers):
        """
        Fetches the content of n randomly selected webpages and their sublinks up to a specified depth

        Args:
            num_samples (int): The number of samples to fetch (if -1, fetch all). Random state is set to 1.
            num_workers (int): The number of workers to use (if depth > 0, 75% of the workers will be used for sublinks. if depth == 0, all workers will be used for the main pages)
        """
        if self.depth == 0:
            municipality_workers = num_workers
        else:
            sublinks_workers = num_workers - math.ceil(num_workers * 0.25)
            municipality_workers = num_workers - sublinks_workers

        sublinks_tasks = []
        self.pbar_sublinks = tqdm(total=0, dynamic_ncols=True)
        asyncio.create_task(self.update_progress(self.pbar_sublinks))

        print(f"Fetching {num_samples} random samples with depth {self.depth} and {num_workers} workers.")

        if num_samples == -1:
            num_samples = len(self.df)
        self.pbar_municipalities = tqdm(total=num_samples, desc="Processing municipalities")

        #self.df = self.df[self.df.website == "http://www.zadorfalva.hu"]
        rows = self.df.dropna(subset=['website']).sample(num_samples, random_state=1)

        print("Starting threads")
        # Start multiple threads to process the sublinks
        if self.depth > 0:
            for _ in range(int(sublinks_workers)):
                task = asyncio.create_task(self.process_sublink())
                sublinks_tasks.append(task)

        # Start fetching the main pages
        print("Starting to fetch main pages")
        municipality_tasks = [self.fetch_municipality(row) for row in rows.iterrows()]
        print(municipality_tasks)
        for i in range(0, len(municipality_tasks), municipality_workers):
            await asyncio.gather(*municipality_tasks[i:i+municipality_workers])
        await asyncio.gather(*sublinks_tasks)

        self.done = True 
        print("done")


def main():
    parser = argparse.ArgumentParser(description="Run the web crawler")
    parser.add_argument("--num_samples", type=int, default=10, help="The number of samples to crawl")
    parser.add_argument("--depth", type=int, default=0, help="The number of depth to crawl sublinks, 0 means only the main page")
    parser.add_argument("--num_workers", type=int, default=10, help="The number of workers to use")
    args = parser.parse_args()

    crawler = WebCrawler(csv_file='../data/urls_processed.csv', depth = args.depth)
    print("Created crawler")
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(crawler.fetch_random(args.num_samples, args.num_workers))
    except KeyboardInterrupt:
        print("Received exit, exiting")
        for task in asyncio.all_tasks(loop):
            task.cancel()
        loop.run_until_complete(asyncio.gather(*asyncio.all_tasks(loop), return_exceptions=True))
        loop.close()

if __name__ == "__main__":
    main()
