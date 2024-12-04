import pandas as pd
import requests
from urllib.parse import urlparse
import feedparser
from tqdm import tqdm
import logging
from bs4 import BeautifulSoup
from urllib.robotparser import RobotFileParser  # Added import
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Configure logging
logging.basicConfig(
    filename="source_list_collector.log",
    level=logging.INFO,
    format="%(asctime)s - %(message)s",
)

# Load the CSV file
sources_df = pd.read_csv("news_websites_modded.csv")

# Initialize the DataFrame to store results
results_df = pd.DataFrame(
    columns=[
        "source_name",
        "domain",
        "country",
        "rss_url",
        "usable_source",
        "is_scraping_allowed",
        "is_domain_up",
        "is_rss_feed_available",
        "is_rss_feed_valid",
    ]
)

# Lock for thread-safe logging
log_lock = threading.Lock()


def validate_url(url):
    try:
        response = requests.head(url, allow_redirects=True, timeout=5)
        return response.status_code == 200
    except requests.RequestException:
        return False


def find_rss_feed(domain):
    potential_feeds = [
        f"https://{domain}/rss",
        f"https://{domain}/feed",
        f"https://{domain}/feeds/posts/default",
        f"https://{domain}/rss.xml",
        f"https://{domain}/feed.xml",
    ]
    for feed_url in potential_feeds:
        try:
            response = requests.get(feed_url, timeout=5)
            if response.status_code == 200:
                return feed_url
        except requests.RequestException as ex:
            continue
    return None


def check_feed_validity(feed_url):
    feed = feedparser.parse(feed_url)
    return feed.bozo == 0


def check_robots_txt(domain, article_links):
    robots_url = f"https://{domain}/robots.txt"
    rp = RobotFileParser()
    rp.set_url(robots_url)
    try:
        rp.read()
        allowed_count = 0
        for link in article_links:
            if rp.can_fetch("*", link):
                allowed_count += 1
        return True, allowed_count / len(article_links)
    except requests.RequestException as ex:
        logging.exception(link)
        return False, 0
    except Exception as ex:
        logging.exception(link)
        return False, 0


def process_source(index, row, pbar):
    source_name = row["publisher_name"]
    country = row["country"]
    link = str(row["link"])  # Ensure link is a string
    domain = urlparse(link).netloc if link else ""

    is_domain_up = validate_url(link) if domain else False
    rss_url = find_rss_feed(domain) if is_domain_up else None
    is_rss_feed_available = rss_url is not None
    is_rss_feed_valid = check_feed_validity(rss_url) if is_rss_feed_available else False

    article_links = []
    if is_rss_feed_valid:
        feed = feedparser.parse(rss_url)
        article_links = [entry.link for entry in feed.entries]

    is_scraping_allowed, scraping_score = (
        check_robots_txt(domain, article_links) if article_links else (False, 0)
    )
    usable_source = is_domain_up and is_rss_feed_valid and is_scraping_allowed

    result = {
        "source_name": source_name,
        "domain": domain,
        "country": country,
        "rss_url": rss_url,
        "usable_source": usable_source,
        "is_scraping_allowed": is_scraping_allowed,
        "is_domain_up": is_domain_up,
        "is_rss_feed_available": is_rss_feed_available,
        "is_rss_feed_valid": is_rss_feed_valid,
    }

    # Thread-safe logging
    with log_lock:
        pbar.write(
            f"{source_name} - Usable: {'\033[0;32mYes' if usable_source else '\033[0;31mNo'} \033[0m"
        )
        pbar.write(
            f"\t\tDomain Status: {'\033[0;32mUp' if is_domain_up else '\033[0;31mDown' :<6}\033[0m RSS feed availability: {'\033[0;32mYes' if is_rss_feed_available else '\033[0;31mNo' :<3}\033[0m Crawling allowed?: {'\033[0;32mYes' if is_scraping_allowed else '\033[0;31mNo' :<5} \033[0m\n"
        )
        pbar.update(1)
        pbar.set_postfix({"Current": index + 1})

    return result


def main():
    total_source_count = sources_df.shape[0]
    usable_source_count = 0

    # Initialize progress bar
    with tqdm(
        total=total_source_count, desc="Processing sources", colour="#903bf8"
    ) as pbar:
        with ThreadPoolExecutor() as executor:
            futures = []
            for index, row in sources_df.iterrows():
                futures.append(executor.submit(process_source, index, row, pbar))

            # Collect results and update the dataframe
            for future in as_completed(futures):
                result = future.result()
                results_df.loc[len(results_df)] = result
                usable_source_count += result["usable_source"]
                logging.info(
                    f"Processed {result['source_name']} - Usable: {result['usable_source']}"
                )

    print(f"Total sources: {total_source_count}")
    print(f"Usable sources: {usable_source_count}")
    print(
        f"Percentage of usable sources: {usable_source_count / total_source_count:.2%}"
    )

    # Export results to CSV
    results_df.to_csv("processed_sources.csv", index=False)


if __name__ == "__main__":
    main()
