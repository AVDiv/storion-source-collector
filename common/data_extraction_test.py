import pandas as pd
from tqdm import tqdm
import feedparser
from newspaper import Article
import logging
from colorama import init, Fore, Style
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import threading

# Initialize colorama
init()

# Configure logging
logging.basicConfig(
    filename="data_extraction.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


class ThreadSafeCounter:
    def __init__(self, initial_value=0):
        self.value = initial_value
        self.lock = Lock()

    def increment(self):
        with self.lock:
            self.value += 1
            return self.value

    def add_to_dict(self, key, dict_obj):
        with self.lock:
            if key not in dict_obj:
                dict_obj[key] = 0
            dict_obj[key] += 1


# Initialize thread-safe counters
counters = {
    "rss_article_link_count": ThreadSafeCounter(),
    "title_extracts": ThreadSafeCounter(),
    "author_extracts": ThreadSafeCounter(),
    "publication_date_extracts": ThreadSafeCounter(),
    "summary_extracts": ThreadSafeCounter(),
    "content_extracts": ThreadSafeCounter(),
    "tag_extracts": ThreadSafeCounter(),
    "failed_scrapes": {},  # Dictionary to store failures per domain
}


def process_article(entry, position, domain):
    link = entry.link
    if not link:
        return None

    try:
        article = Article(link)
        article.download()
        article.parse()

        results = {key: 0 for key in counters.keys()}
        results["rss_article_link_count"] = 1

        if article.title:
            results["title_extracts"] = 1
        if article.authors:
            results["author_extracts"] = 1
        if article.publish_date:
            results["publication_date_extracts"] = 1
        if article.summary:
            results["summary_extracts"] = 1
        if article.text:
            results["content_extracts"] = 1
        if article.tags:
            results["tag_extracts"] = 1

        return results

    except Exception as e:
        tqdm.write(
            f"{Fore.YELLOW}Error processing article {link}: {str(e)}{Style.RESET_ALL}"
        )
        logging.error(f"Error processing article {link}: {str(e)}")
        counters["rss_article_link_count"].add_to_dict(
            domain, counters["failed_scrapes"]
        )
        return None


def process_feed(item, index, position):
    domain = item["domain"]
    tqdm.write(f"{Fore.CYAN}Processing: {item['title']}{Style.RESET_ALL}")
    logging.info(f"Processing source: {item['title']}")

    results = {
        "name": item["title"],
        "domain": item["domain"],
        "valid_feed": False,
        "rss_entry_count": 0,
    }

    rss_feed = feedparser.parse(item["rss"])
    is_valid_feed = not rss_feed.bozo
    results["valid_feed"] = is_valid_feed

    if not is_valid_feed:
        tqdm.write(f"{Fore.RED}Invalid feed: {item['domain']}{Style.RESET_ALL}")
        logging.warning(f"Invalid feed: {item['domain']}")
        return results

    entries = rss_feed.entries
    results["rss_entry_count"] = len(entries)

    failed_count = 0
    # Process articles in parallel
    with ThreadPoolExecutor(max_workers=5) as executor:
        article_futures = {
            executor.submit(process_article, entry, position + 1, domain): entry
            for entry in entries
        }

        for future in tqdm(
            as_completed(article_futures),
            total=len(entries),
            desc=f"Processing articles for {item['domain']}",
            position=position + 1,
            leave=False,
        ):
            article_results = future.result()
            if article_results:
                for key, value in article_results.items():
                    if value > 0:
                        counters[key].increment()
            else:
                failed_count += 1

    results["failed_scrapes"] = failed_count
    return results


# Replace the main processing loop
df = pd.read_csv("../final_sources.csv")
source_count = len(df)

all_results = []
with ThreadPoolExecutor(max_workers=3) as executor:
    feed_futures = {
        executor.submit(process_feed, row, idx, 0): idx for idx, row in df.iterrows()
    }

    for future in tqdm(
        as_completed(feed_futures),
        total=source_count,
        desc="Processing sources",
        position=0,
    ):
        all_results.append(future.result())

# Create final results DataFrame
results_df = pd.DataFrame(all_results)
for key in counters.keys():
    if key != "failed_scrapes":  # Handle failed_scrapes separately
        results_df[key] = [counters[key].value] * len(results_df)

# Add failed scrapes count for each domain
results_df["failed_scrapes"] = results_df["domain"].map(
    lambda x: counters["failed_scrapes"].get(x, 0)
)

results_df.to_csv("data_extraction_test_results.csv", index=False)
