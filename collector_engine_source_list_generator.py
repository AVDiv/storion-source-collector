import pandas as pd
import requests
from urllib.parse import urlparse
import feedparser
from tqdm import tqdm
import logging
from bs4 import BeautifulSoup
from urllib.robotparser import RobotFileParser  # Added import

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


def validate_url(url):
    try:
        response = requests.head(url, allow_redirects=True, timeout=5)
        return response.status_code == 200
    except requests.RequestException:
        return False


def find_rss_feed(domain):
    potential_feeds = [
        f"{domain}/rss",
        f"{domain}/feed",
        f"{domain}/feeds/posts/default",
        f"{domain}/rss.xml",
        f"{domain}/feed.xml",
    ]
    for feed_url in potential_feeds:
        try:
            response = requests.get(feed_url, timeout=5)
            if response.status_code == 200:
                return feed_url
        except requests.RequestException:
            continue
    return None


def check_feed_validity(feed_url):
    feed = feedparser.parse(feed_url)
    return feed.bozo == 0


def check_robots_txt(domain, article_links):
    robots_url = f"{domain}/robots.txt"
    rp = RobotFileParser()
    rp.set_url(robots_url)
    try:
        rp.read()
        allowed_count = 0
        for link in article_links:
            if rp.can_fetch("*", link):
                allowed_count += 1
        return True, allowed_count / len(article_links)
    except requests.RequestException:
        return False, 0


for index, row in tqdm(
    sources_df.iterrows(), total=sources_df.shape[0], desc="Processing sources"
):
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

    results_df = results_df._append(
        {
            "source_name": source_name,
            "domain": domain,
            "country": country,
            "rss_url": rss_url,
            "usable_source": usable_source,
            "is_scraping_allowed": is_scraping_allowed,
            "is_domain_up": is_domain_up,
            "is_rss_feed_available": is_rss_feed_available,
            "is_rss_feed_valid": is_rss_feed_valid,
        },
        ignore_index=True,
    )

    logging.info(f"Processed {source_name} - Usable: {usable_source}")

# Export results to CSV
results_df.to_csv("processed_sources.csv", index=False)
