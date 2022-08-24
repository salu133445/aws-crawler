"""Crawl the data."""
import argparse
import json
import logging
import pathlib
import sys
import time
from typing import Callable, Union

import boto3
import tqdm


def parse_args(args=None, namespace=None):
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-f",
        "--function_name",
        default="crawler-dev-crawl",
        help="AWS Lambda function name",
    )
    parser.add_argument(
        "-i",
        "--in_filename",
        default=pathlib.Path(__file__).parent / "urls.txt",
        type=pathlib.Path,
        help="input filename that contains the URLs to crawl",
    )
    parser.add_argument(
        "-o",
        "--out_dir",
        default=pathlib.Path(__file__).parent / "results",
        type=pathlib.Path,
        help="output directory to store the crawled data",
    )
    parser.add_argument("-p", "--profile", help="AWS profile")
    parser.add_argument("-r", "--region", help="AWS Lambda region")
    parser.add_argument(
        "-q", "--quiet", action="store_true", help="show warnings only"
    )
    return parser.parse_args(args=args, namespace=namespace)


def setup_loggers(log_dir: str, quiet: bool):
    """Set up the loggers."""
    # Set up a file logger
    logging.basicConfig(
        filename=pathlib.Path(log_dir) / "crawl.log",
        filemode="a",
        level=logging.DEBUG,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Set up a console logger
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.WARNING if quiet else logging.INFO)
    stream_handler.setFormatter(logging.Formatter("%(message)s"))
    logging.getLogger().addHandler(stream_handler)

    # Suppress AWS logging
    logging.getLogger("boto3").setLevel(logging.ERROR)
    logging.getLogger("botocore").setLevel(logging.ERROR)
    logging.getLogger("urllib3").setLevel(logging.ERROR)


def example_name_func(url):
    """Return the name of file given a URL."""
    return f"{url.split('/')[-1]}.json"


class Crawler:
    def __init__(
        self,
        function_name: str,
        in_filename: Union[str, pathlib.Path],
        out_dir: Union[str, pathlib.Path],
        name_func: Callable,
    ):
        logging.info("Creating the crawler...")
        self.function_name = function_name
        self.out_dir = pathlib.Path(out_dir)
        self.name_func = name_func

        # Get the Lambda client
        self.client = boto3.client("lambda")

        # Get URLs
        self.urls = []
        with open(pathlib.Path(in_filename)) as f:
            for line in f:
                self.urls.append(line.strip())

        # Make sure the crawled directory exists
        self.crawled_dir = self.out_dir / "crawled"
        self.crawled_dir.mkdir(exist_ok=True)

        # Set up a file to keep track of the crawled URLs
        crawled_urls_filename = self.out_dir / "crawled-urls.txt"

        # Load the crawled URLs
        if crawled_urls_filename.is_file():
            with open(crawled_urls_filename) as f:
                self.crawled_urls = set(line.strip() for line in f)
        else:
            # Create an empty file if it does not exist
            crawled_urls_filename.touch()
            # Set crawled URLs to an empty set
            self.crawled_urls = set()

        # Set up a file to keep track of the failed URLs
        failed_urls_filename = self.out_dir / "failed-urls.txt"

        # Load the failed URLs
        if failed_urls_filename.is_file():
            with open(failed_urls_filename) as f:
                self.failed_urls = set(line.split(",")[0] for line in f)
        else:
            # Create an empty file if it does not exist
            failed_urls_filename.touch()
            # Set failed URLs to an empty set
            self.failed_urls = set()

        # Open the files in append mode
        self.crawled_urls_file = open(crawled_urls_filename, "a")
        self.failed_urls_file = open(failed_urls_filename, "a")

    def reset_client(self):
        """Reset the client (a new IP will be assigned)."""
        logging.debug("Resetting the crawler...")
        # Update the function configuration
        # (This will force the client to cold start and get a new IP.)
        self.client.update_function_configuration(
            FunctionName=self.function_name,
            Description=f"Crawler-{int(time.time())}",
        )
        # Wait a while for it to take effect
        time.sleep(60)

    def crawl(self, url: str, test: bool = False):
        """Crawl a URL."""
        # Invoke Lambda function
        response = self.client.invoke(
            FunctionName=self.function_name,
            Payload=f'{{"url": "{url}"}}',
        )

        # Log the failure with its error message
        if response["StatusCode"] != 200:
            logging.debug(
                f"Failed on {url} with the following error:\n"
                f"{response['FunctionError']}"
            )
            return response["StatusCode"]

        # Read the return payload
        payload = response["Payload"].read().decode("utf-8")

        # Parse the output into a dictionary (assuming JSON format)
        data = json.loads(payload)

        # Handle bad return payload
        if "status_code" not in data:
            logging.debug(
                f"Failed on {url} with bad return payload: {payload}"
            )
            return response["StatusCode"]

        # Handle failed requests
        if data["status_code"] != 200:
            # Log the failure with its status code
            logging.debug(
                f"Failed on {url} with status code: {data['status_code']}"
            )

            # Record failed URLs (except 403 --> likely got banned)
            if not test and data["status_code"] != 403:
                self.failed_urls_file.write(f"{url},{data['status_code']}\n")

            return data["status_code"]

        # Save successful request response to file
        with open(self.crawled_dir / self.name_func(url), "w") as f:
            json.dump(data, f)

        # Record crawled URLs
        if not test:
            self.crawled_urls_file.write(f"{url}\n")

        return data["status_code"]

    def crawl_all(
        self,
        sleep: int = 0,
        test: bool = False,
        max_requests_per_restart=1000,
        max_forbidden_per_restart=10,
    ):
        """Crawl all the URLs."""
        # Initialize counters
        count_requests = 0
        count_forbidden = 0

        # Iterate over all the URLs
        logging.info("Start crawling...")
        for url in (pbar := tqdm.tqdm(self.urls, ncols=120)):
            # Skip the URL if it has been crawled or once failed
            if url in self.crawled_urls or url in self.failed_urls:
                continue

            # Crawl the URL
            status_code = self.crawl(url, test=test)

            # Append the latest status code to the progress bar
            pbar.set_postfix(
                status_code=status_code,
                count_requests=count_requests,
                count_forbidden=count_forbidden,
            )

            # Handle forbidden requests
            if status_code == 403:
                # Increment forbidden request counter
                count_forbidden += 1
                # Reset the client if we get many forbidden requests
                if count_forbidden >= max_forbidden_per_restart:
                    logging.debug(
                        f"Got {max_forbidden_per_restart} forbidden requests "
                        "in this session"
                    )
                    self.reset_client()

                    # Reset counters
                    count_forbidden = 0
                    count_requests = 0
                continue

            # Increment request counter
            count_requests += 1

            # Reset the crawler once in a while to get a new IP address
            if count_requests >= max_requests_per_restart:
                logging.debug(
                    f"Sent {max_requests_per_restart} requests in this session"
                )
                self.reset_client()

                # Reset counters
                count_forbidden = 0
                count_requests = 0

            # Sleep for a certain seconds
            if sleep:
                time.sleep(sleep)

    def close(self):
        """Close the opened files."""
        self.crawled_urls_file.close()
        self.failed_urls_file.close()
        logging.info(f"Closed the crawler")


def main():
    """Main function."""
    # Parse the command-line arguments
    args = parse_args()

    # Make sure the output directory exists
    args.out_dir.mkdir(exist_ok=True)

    # Set up loggers
    setup_loggers(args.out_dir, args.quiet)

    # Set up AWS profile and region
    boto3.setup_default_session(
        profile_name=args.profile, region_name=args.region
    )

    # Create the crawler
    crawler = Crawler(
        function_name=args.function_name,
        in_filename=args.in_filename,
        out_dir=args.out_dir,
        name_func=example_name_func,
    )

    # Crawl the data
    crawler.crawl_all()

    # Close the opened files
    crawler.close()


if __name__ == "__main__":
    main()
