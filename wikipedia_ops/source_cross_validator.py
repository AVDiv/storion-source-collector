import pandas as pd
import requests
from urllib.parse import urlparse
from tqdm import tqdm
import logging

# Configure logging
logging.basicConfig(
    filename="site_validation.log",
    level=logging.INFO,
    format="%(asctime)s - %(message)s",
)

# Load the CSV files
news_websites_df = pd.read_csv("news_websites(wikipedia).csv")
brave_sources_df = pd.read_csv("initial_sources.csv")

# Count the number of sources with a web URL in news_websites(wikipedia).csv
unique_sources_df = news_websites_df[news_websites_df["Link"].notnull()]
unique_sources_df = unique_sources_df.drop_duplicates(subset="Link", keep="first")
num_sources_with_url = unique_sources_df.shape[0]
print(
    f"Number of sources with a web URL (& non-duplicates): {num_sources_with_url} / {len(news_websites_df)}"
)
logging.info(
    f"Number of sources with a web URL (& non-duplicates): {num_sources_with_url} / {len(news_websites_df)}"
)

# Extract domain names from URLs
news_websites_df["domain"] = news_websites_df["Link"].apply(
    lambda x: urlparse(x).netloc if pd.notnull(x) else None
)
brave_sources_df["domain"] = brave_sources_df["domain"].apply(
    lambda x: urlparse(x).netloc if pd.notnull(x) else None
)

# Find common sources by domain name
common_domains = set(news_websites_df["domain"]).intersection(
    set(brave_sources_df["domain"])
)
num_common_sources = len(common_domains)
print(f"Number of common sources: {num_common_sources}")
logging.info(f"Number of common sources: {num_common_sources}")


# Validate the URLs by checking if they are accessible
def validate_url(url):
    try:
        response = requests.head(url, allow_redirects=True, timeout=5)
        return response.status_code == 200
    except requests.RequestException:
        return False


# Apply validation with progress bar
valid_urls = []
error_examples = {}
i = 0
for url in tqdm(
    unique_sources_df["Link"],
    desc="Validating URLs",
):
    result = validate_url(url)
    if not isinstance(result, bool):
        valid_urls.append(False)
        error_examples[result] = news_websites_df.iloc[i, 0]
        logging.info(f"URL: {url} - Status Code: {result}")
    else:
        valid_urls.append(result)
        logging.info(f"URL: {url} - Valid: {result}")
    i += 1

num_valid_urls = sum(valid_urls)
print()
print(f"Number of valid URLs: {num_valid_urls}")
logging.info(f"Number of valid URLs: {num_valid_urls}")
print("Error examples: ")
logging.info("Error examples: ")
for status_code, source in error_examples.items():
    print(f"  {status_code}: {source}")
    logging.info(f"  {status_code}: {source}")
