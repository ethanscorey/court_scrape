import asyncio
import time
import random
import os
import re

import aiohttp
import aiofiles
from bs4 import BeautifulSoup

from rate_limiter import RateLimiter


class AsyncioOrleansScraper:
    """Custom scraper for Orleans Parish Website. Docket sheets are
    stored as preformatted text at
    http://www.opcso.org/dcktmstr/666666.php?&docase=######, where
    ###### is a six-digit number between 328010 and 547817.

    Defendant info is stored as HTML at
    http://www.opcso.org/dcktmstr/dmdspscn.php?d1scnn=######, where
    ###### is a six-digit number between 633000 and 791264.
    """
    def_url = 'http://www.opcso.org/dcktmstr/dmdspscn.php?d1scnn='
    sheet_url = 'http://www.opcso.org/dcktmstr/'

    def __init__(self,
                 start_num=633000,
                 end_num=791264,
                 defendant_path='defendants/',
                 docket_path='dockets/',
                 log_file='log.txt',
                 max_tokens=10,
                 rate=10,
                 verbose=False):
        self.start_num = start_num
        self.end_num = end_num
        self.defendant_path = defendant_path
        self.docket_path = docket_path
        self.failed_urls = []
        self.completed_defendants = []
        self.log_file = log_file
        self.log_buffer = ''
        self.log_len = 0
        self.max_tokens = max_tokens
        self.rate = rate
        self.verbose = verbose
        self.links = set()

    def log(self, message):
        if self.verbose:
            print(time.strftime('%c'))
            print(message)

        self.log_buffer += time.strftime('%c')
        self.log_buffer += f"\n{message}\n"

        if self.log_len > 100000:
            self.write_log()

    def write_log(self):
        with open(self.log_file, 'a') as f:
            f.write(self.log_buffer)
            self.log_buffer = ''
            self.log_len = 0

    async def download_item(self, session, url_root, item):
        self.log(f"downloading {item}")
        url = f"{url_root}{item}"
        try:
            async with await session.get(url) as response:
                return await response.text(errors='ignore')
        except (asyncio.TimeoutError, aiohttp.ClientError) as e:
            self.log(f"Timeout/ClientError downloading item {url}: {type(e)}")
            self.failed_urls.append(url)
            return
        except Exception as e:
            self.failed_urls += url
            self.log(f"Exception downloading item {url}: {type(e)}")
            raise e

    async def download_defendant(self, session, defendant):
        def_data = await self.download_item(session,
                                            self.def_url,
                                            defendant)

        if def_data:
            return BeautifulSoup(def_data, 'html.parser')
        else:
            return None

    async def download_sheet(self, session, link):
        return await self.download_item(session,
                                        self.sheet_url,
                                        link)

    async def write_data(self, data, fname):
        self.log(f"Writing {fname} of length: {len(str(data))}")
        async with aiofiles.open(fname, 'wb') as f:
            if data:
                await f.write(str(data).encode('utf-8'))

    @staticmethod
    def get_defendant_case_links(soup):
        hrefs = [a['href'] for a in soup.find_all('a')]
        return set(href for href in hrefs if '666666.php' in href)

    async def download_defendant_data(self, session, defendant):
        try:
            fname = f"{self.defendant_path}defendant{defendant}.html"
            defendant_data = await self.download_defendant(session, defendant)
            if defendant_data:
                await self.write_data(defendant_data, fname)
                links = self.get_defendant_case_links(defendant_data)
                self.links = self.links.union(links)
        except Exception as e:
            self.log(f"Exception with defendant {defendant}: {type(e)}")
            raise e

    async def download_sheet_data(self, session, link):
        try:
            fname = f"{self.docket_path}docket{link[-6:]}.html"
            sheet_data = await self.download_sheet(session, link)
            if sheet_data:
                await self.write_data(sheet_data, fname)
        except Exception as e:
            self.log(f"Exception with sheet {link}: {type(e)}")
            raise e

    @staticmethod
    def check_missing(start, stop, path, fpattern):
        downloaded = [int(i[:-5][-6:]) for i in os.listdir(path)
                      if re.search(fpattern, i)]
        return [j for j in range(start, stop) if j not in downloaded]

    def check_missing_defendants(self, start, stop):
        return self.check_missing(start,
                                  stop,
                                  self.defendant_path,
                                  r'defendant\d+\.html')

    def check_missing_sheets(self, start, stop):
        return self.check_missing(start,
                                  stop,
                                  self.docket_path,
                                  r'docket\d+\.html')

    @staticmethod
    def check_range_downloaded(start, stop, path, fpattern):
        downloaded = [int(i[:-5][-6:]) for i in os.listdir(path)
                      if re.search(fpattern, i)]
        return start in downloaded or stop in downloaded

    def check_defendant_range_downloaded(self, start, stop):
        return self.check_range_downloaded(start,
                                           stop,
                                           self.defendant_path,
                                           r'defendant\d+\.html')

    def check_sheet_range_downloaded(self, start, stop):
        return self.check_range_downloaded(start,
                                           stop,
                                           self.docket_path,
                                           r'docket\d+\.html')

    async def munch_defendants(self, defendants):
        self.log(f"Munching started")
        timeout = aiohttp.ClientTimeout(total=None)
        async with aiohttp.ClientSession(
                headers={"Connection": "close"},
                timeout=timeout
        ) as session:
            session = RateLimiter(session, self.rate, self.max_tokens)
            await asyncio.gather(
                *[self.download_defendant_data(
                    session,
                    defendant
                )
                    for defendant in defendants]
            )

            await asyncio.gather(
                *[self.download_sheet_data(
                    session,
                    link
                )
                    for link in self.links]
            )
            self.links = set()
            self.log("Munching completed")

    async def munch_missing(self, start, stop):
        defendants = self.check_missing_defendants(start, stop)
        await self.munch_defendants(defendants)

    async def munch(self, start, stop, verbose=False):
        self.verbose = verbose
        try:
            if self.check_defendant_range_downloaded(start, stop):
                await self.munch_missing(start, stop)
            else:
                defendants = list(range(start, stop))
                random.shuffle(defendants)
                await self.munch_defendants(defendants)
        except Exception as e:
            self.log(f"Munching cancelled due to exception: {e}")
            self.write_log()
        self.write_log()
