import argparse
import asyncio
import logging
import sys

from crawler import Crawler

ARGS = argparse.ArgumentParser(description="Web crawler")
ARGS.add_argument(
    '-u', dest='root',
    default=True, help='Website URL to be crawled')
ARGS.add_argument(
    '-f', dest='file',
    default=True, help='CSV file path for output')
ARGS.add_argument(
    '-c', dest='max_tasks', type=int,
    default=False, help='Max number of concurrent tasks')


def main():
    logging.basicConfig(level=logging.DEBUG)
    loop = asyncio.get_event_loop()

    args = ARGS.parse_args()
    if not args.root:
        print('Use --help for command line help')
        return

    if not args.root.startswith('http://'):
        args.root = 'http://{}'.format(args.root)

    if not args.max_tasks:
        args.max_tasks = 1000

    crawler = Crawler(args.root, max_tasks=args.max_tasks, file=args.file)
    try:
        loop.run_until_complete(crawler.crawl())
    except KeyboardInterrupt:
        sys.stderr.flush()
        print('\nInterrupted\n')
    finally:
        crawler.close()
        loop.stop()
        loop.run_forever()

        loop.close()


if __name__ == '__main__':
    main()
