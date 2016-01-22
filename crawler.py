import asyncio
import cgi
import csv
import time
import logging
import re
import urllib.parse

import aiohttp

try:
    # Python 3.4.
    from asyncio import JoinableQueue as Queue
except ImportError:
    # Python 3.5.
    from asyncio import Queue

LOGGER = logging.getLogger(__name__)

CSV_HEADER = ['URL', 'Status Code']


def is_redirect(response):
    return response.status in (300, 301, 302, 303, 307)


class Crawler:
    def __init__(self, root, max_tasks=1000, loop=None, file=None):
        LOGGER.info('Starting Crawler ...\n')
        self.loop = loop or asyncio.get_event_loop()
        self.q = Queue(loop=self.loop)
        self.visited_urls = set()
        self.max_tasks = max_tasks
        self.session = aiohttp.ClientSession(loop=self.loop)
        self.root_domains = set()

        parts = urllib.parse.urlparse(root)
        host, port = urllib.parse.splitport(parts.netloc)
        if re.match(r'\A[\d\.]*\Z', host):
            self.root_domains.add(host)
        else:
            host = host.lower()
            self.root_domains.add(host)

        print('Hosts : {}'.format(','.join(self.root_domains)))

        self.add_url(root)

        self.t0 = time.time()
        self.t1 = None
        filename = '{}.csv'.format(file)
        self.f = open(filename, 'w')
        self.csv = csv.writer(self.f)
        self.csv.writerow(CSV_HEADER)

    def add_url(self, url):
        LOGGER.debug('adding %r', url)
        self.visited_urls.add(url)
        self.q.put_nowait(url)

    def close(self):
        self.session.close()
        self.f.close()

    def host_okay(self, host):
        host = host.lower()
        if host in self.root_domains:
            return True
        if re.match(r'\A[\d\.]*\Z', host):
            return False
        return self._host_okay_strict(host)

    def _host_okay_strict(self, host):
        host = host[4:] if host.startswith('www.') else 'www.' + host
        return host in self.root_domains

    def url_allowed(self, url):
        parts = urllib.parse.urlparse(url)
        if parts.scheme not in ('http', 'https'):
            LOGGER.debug('skipping non-http scheme in %r', url)
            return False
        host, port = urllib.parse.splitport(parts.netloc)
        if not self.host_okay(host):
            LOGGER.debug('skipping non-root host in %r', url)
            return False
        return True

    @asyncio.coroutine
    def parse_response(self, response):
        links = set()
        if response.status == 200:
            content_type = response.headers.get('content-type')

            if content_type:
                content_type, pdict = cgi.parse_header(content_type)

            if content_type in ('text/html', 'application/xml'):
                text = yield from response.text()
                urls = set(re.findall(r'''(?i)href=["']([^\s"'<>]+)''',
                                      text))
                if urls:
                    LOGGER.info('got %r distinct urls from %r',
                                len(urls), response.url)
                for url in urls:
                    normalized = urllib.parse.urljoin(response.url, url)
                    defragmented, frag = urllib.parse.urldefrag(normalized)
                    if self.url_allowed(defragmented):
                        links.add(defragmented)

                if links:
                    LOGGER.info('got %r distinct urls from %r', len(links), response.url)
                for link in links.difference(self.visited_urls):
                    self.q.put_nowait(link)
                self.visited_urls.update(links)
        return links

    @asyncio.coroutine
    def fetch(self, url):
        try:
            response = yield from self.session.get(url, allow_redirects=False)
            self.csv.writerow([url, response.status])

            if is_redirect(response):
                location = response.headers['location']
                next_url = urllib.parse.urljoin(url, location)
                if next_url in self.visited_urls:
                    return
                else:
                    self.add_url(next_url)
            else:
                links = yield from self.parse_response(response)

                for link in links.difference(self.visited_urls):
                    self.q.put_nowait(link)
                self.visited_urls.update(links)
            yield from response.release()
        except aiohttp.ClientError as client_error:
            LOGGER.info('try for %r raised %r', url, client_error)

    @asyncio.coroutine
    def work(self):
        try:
            while True:
                url = yield from self.q.get()
                assert url in self.visited_urls
                yield from self.fetch(url)
                self.q.task_done()
        except asyncio.CancelledError:
            pass

    @asyncio.coroutine
    def crawl(self):
        workers = [asyncio.Task(self.work(), loop=self.loop) for _ in range(self.max_tasks)]
        self.t0 = time.time()
        yield from self.q.join()
        self.t1 = time.time()
        for w in workers:
            w.cancel()
