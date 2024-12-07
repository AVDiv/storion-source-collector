import pandas as pd
from tqdm import tqdm
import feedparser
from newspaper import Article
import logging
from colorama import init, Fore, Style
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import sys
import traceback

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
    link = entry.link if hasattr(entry, "link") else None
    if not link:
        logging.warning(f"No link found in entry for domain: {domain}")
        return None

    try:
        article = Article(link)
        article.download()
        article.parse()

        results = {key: 0 for key in counters.keys()}
        results["rss_article_link_count"] = 1

        # Safely check article attributes
        try:
            if getattr(article, "title", None):
                results["title_extracts"] = 1
            if getattr(article, "authors", None):
                results["author_extracts"] = 1
            if getattr(article, "publish_date", None):
                results["publication_date_extracts"] = 1
            if getattr(article, "summary", None):
                results["summary_extracts"] = 1
            if getattr(article, "text", None):
                results["content_extracts"] = 1
            if getattr(article, "tags", None):
                results["tag_extracts"] = 1
        except Exception as e:
            logging.error(f"Error accessing article attributes for {link}: {str(e)}")

        return results

    except Exception as e:
        error_trace = traceback.format_exc()
        tqdm.write(
            f"{Fore.YELLOW}Error processing article {link}: {str(e)}{Style.RESET_ALL}"
        )
        logging.error(f"Error processing article {link}: {str(e)}\n{error_trace}")
        counters["rss_article_link_count"].add_to_dict(
            domain, counters["failed_scrapes"]
        )
        return None


def process_feed(item, index, position):
    try:
        domain = item.get("domain", "unknown_domain")
        title = item.get("title", "unknown_title")
        rss_url = item.get("rss", "")

        if not rss_url:
            logging.error(f"Missing RSS URL for {domain}")
            return create_error_result(title, domain)

        tqdm.write(f"{Fore.CYAN}Processing: {title}{Style.RESET_ALL}")
        logging.info(f"Processing source: {title}")

        results = {
            "name": title,
            "domain": domain,
            "valid_feed": False,
            "rss_entry_count": 0,
            "failed_scrapes": 0,
        }

        try:
            rss_feed = feedparser.parse(rss_url)
        except Exception as e:
            logging.error(f"Feed parsing error for {domain}: {str(e)}")
            return create_error_result(title, domain)

        is_valid_feed = not rss_feed.bozo
        results["valid_feed"] = is_valid_feed

        if not is_valid_feed:
            tqdm.write(f"{Fore.RED}Invalid feed: {domain}{Style.RESET_ALL}")
            logging.warning(f"Invalid feed: {domain}")
            return results

        entries = rss_feed.entries
        results["rss_entry_count"] = len(entries)

        process_articles_with_threadpool(entries, position, domain, results)
        return results

    except Exception as e:
        error_trace = traceback.format_exc()
        logging.error(f"Error in process_feed: {str(e)}\n{error_trace}")
        return create_error_result(
            item.get("title", "unknown"), item.get("domain", "unknown")
        )


def create_error_result(title, domain):
    return {
        "name": title,
        "domain": domain,
        "valid_feed": False,
        "rss_entry_count": 0,
        "failed_scrapes": 0,
    }


def process_articles_with_threadpool(entries, position, domain, results):
    try:
        with ThreadPoolExecutor(max_workers=5) as executor:
            article_futures = {
                executor.submit(process_article, entry, position + 1, domain): entry
                for entry in entries
            }

            failed_count = 0
            for future in tqdm(
                as_completed(article_futures),
                total=len(entries),
                desc=f"Processing articles for {domain}",
                position=position + 1,
                leave=False,
            ):
                try:
                    article_results = future.result()
                    if article_results:
                        for key, value in article_results.items():
                            if value > 0:
                                counters[key].increment()
                    else:
                        failed_count += 1
                except Exception as e:
                    logging.error(f"Error processing future result: {str(e)}")
                    failed_count += 1

            results["failed_scrapes"] = failed_count
    except Exception as e:
        logging.error(f"Thread pool error for {domain}: {str(e)}")
        results["failed_scrapes"] = len(entries)


# Main execution
try:
    df = pd.read_csv("../final_sources.csv")
except Exception as e:
    logging.critical(f"Failed to read source CSV file: {str(e)}")
    sys.exit(1)

try:
    source_count = len(df)
    all_results = []

    with ThreadPoolExecutor(max_workers=3) as executor:
        feed_futures = {
            executor.submit(process_feed, row, idx, 0): idx
            for idx, row in df.iterrows()
        }

        for future in tqdm(
            as_completed(feed_futures),
            total=source_count,
            desc="Processing sources",
            position=0,
        ):
            try:
                result = future.result()
                all_results.append(result)
            except Exception as e:
                logging.error(f"Error processing feed future: {str(e)}")
                all_results.append(create_error_result("error", "unknown"))

    # Create final results DataFrame
    results_df = pd.DataFrame(all_results)
    for key in counters.keys():
        if key != "failed_scrapes":
            results_df[key] = [counters[key].value] * len(results_df)

    results_df["failed_scrapes"] = results_df["domain"].map(
        lambda x: counters["failed_scrapes"].get(x, 0)
    )

    try:
        results_df.to_csv("data_extraction_test_results.csv", index=False)
    except Exception as e:
        logging.error(f"Failed to save results CSV: {str(e)}")
        # Save to alternate location
        results_df.to_csv("/tmp/data_extraction_test_results_backup.csv", index=False)

except Exception as e:
    error_trace = traceback.format_exc()
    logging.critical(f"Critical error in main execution: {str(e)}\n{error_trace}")
    sys.exit(1)
