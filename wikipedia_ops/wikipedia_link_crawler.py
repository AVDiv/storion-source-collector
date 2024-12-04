from lxml import html
import requests
import csv
import time
import pickle

wikipedia_main_page = "https://en.wikipedia.org/wiki/Category:News_websites_by_country"
wikipedia_domain = "https://en.wikipedia.org"
csvfile = open("news_websites(wikipedia).csv", "w", encoding="utf-8")


def update_progress_bar(progress, total, msg=""):
    progress_percentage = (progress / total) * 100
    bar_length = 40
    block = int(round(bar_length * progress_percentage / 100))
    progress_bar = (
        "\033[1;32m" + "#" * block + "\033[1;33m" + "-" * (bar_length - block)
    )
    if progress_percentage == 100:
        msg = " Done!"
    else:
        msg = f" {msg}"
    output = f"\033[1;34mProgress: [{progress_bar}] {progress_percentage:>6.2f}% | {progress:>4}/{total} items |{msg}"
    print(output + " " * (300 - len(output)), end="\r")
    if progress_percentage == 100:
        print()


def print_log(message, checkbox=False):
    prefix = "\033[1;34m[\033[1;32m✓\033[1;34m] " if checkbox else "\033[1;34m"
    print(f"{prefix}{message}\033[0m")


def print_inline_log(message, checkbox=False):
    prefix = "\033[1;34m[ ] " if checkbox else "\033[1;34m"
    print(f"{prefix}{message}\033[0m", end="\r")


def print_error(message):
    print(f"\033[1;31m[ERROR] {message}\033[0m")


def remove_duplicates_of_list(lst):
    prev = ""
    links_size = len(lst)
    for i, lst_item in enumerate(lst[::-1]):
        if lst_item == prev:
            lst.pop(links_size - i + 1)
        prev = lst_item
    return lst


def get_country_links():
    print_inline_log("Fetching country links from Wikipedia...", checkbox=True)
    response = requests.get(wikipedia_main_page)
    if response.status_code != 200:
        print_error(f"Failed to retrieve the main page: {response.status_code}")
        return []

    tree = html.fromstring(response.text)
    country_links = [
        wikipedia_domain + link
        for link in tree.xpath('//div[@class="mw-category-group"]//a/@href')
    ]
    country_links = remove_duplicates_of_list(country_links)
    country_names = tree.xpath('//div[@class="mw-category-group"]//a/text()')
    country_sites = dict(zip(country_names, country_links))
    print_log(
        f"Retrieved {len(country_links)} country links.               ", checkbox=True
    )
    return country_sites.items()


def get_news_website_links(country_name, country_link, writer):
    print_log(f"Fetching news websites for {country_name}...")
    param_names = ("website", "url", "site", "link", "domain", "web")
    response = requests.get(country_link)
    if response.status_code != 200:
        print_error(f"Failed to retrieve {country_name} links: {response.status_code}")
        return

    tree = html.fromstring(response.text)
    news_website_names = tree.xpath('//div[@class="mw-category-group"]//a/text()')
    news_website_links = [
        wikipedia_domain + link
        for link in tree.xpath('//div[@class="mw-category-group"]//a/@href')
    ]

    if len(news_website_names) != len(news_website_links):
        news_website_links = remove_duplicates_of_list(news_website_links)

    if len(news_website_names) != len(news_website_links):
        print_error(
            f"Failed to retrieve {country_name} news websites: Name and link count mismatch"
        )
        # Serialize the variables
        with open(f"news_website_data({country_name.lower()}).pkl", "wb") as f:
            pickle.dump((news_website_names, news_website_links), f)
        return

    total = len(news_website_links)
    for i, link in enumerate(news_website_links):
        response = requests.get(link)
        tree = html.fromstring(response.text)
        website_link = tree.xpath(
            '//td[contains(concat(" ", normalize-space(@class), " "), " infobox-data ")]//a[contains(@href, "http")]/@href'
        )
        if len(website_link) == 0:
            website_link = tree.xpath("//a[text()='Official Website']/@href")
            website_link = website_link[0] if len(website_link) > 0 else "N/A"
        else:
            website_link = website_link[-1]
        writer.write(
            f"{news_website_names[i].encode('utf-8').decode('utf-8')},{website_link.encode('utf-8').decode('utf-8')},{country_name.encode('utf-8').decode('utf-8')}\n"
        )
        update_progress_bar(
            i + 1, total, msg=f"Fetching {country_name} news websites..."
        )


def get_news_website_links_from_all_countries():
    global csvfile
    print_log("Starting the collection of news website links from all countries...")
    country_links = get_country_links()

    for country_name, country_link in country_links:
        get_news_website_links(country_name, country_link, csvfile)

    print_log(
        "Finished collecting news websites.", checkbox=True
    )  # Clear line after overwriting


if __name__ == "__main__":
    try:
        start_time = time.time()  # Start timer
        csvfile.write("Name,Link,Country\n")
        get_news_website_links_from_all_countries()

        end_time = time.time()  # End timer
        elapsed_time = end_time - start_time
        print_log(
            f"Export completed! Total time taken: {elapsed_time:.2f} seconds."
        )  # Clear line after overwriting
    except Exception as e:
        print_error(f"An error occurred: {e}")
    finally:
        csvfile.close()
