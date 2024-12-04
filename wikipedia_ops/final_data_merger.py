import pandas as pd

initial_df = pd.read_csv("../initial_sources_filtered.csv")
processed_df = pd.read_csv("./processed_sources.csv")

processed_df = processed_df[processed_df["usable_source"] == True]

# Merge the two dataframes
initial_duplicate_count = initial_df.duplicated(subset=["domain"]).sum()
new_duplicate_count = processed_df.duplicated(subset=["domain"]).sum()

merged_duplicate_count = (
    pd.concat([initial_df, processed_df]).duplicated(subset=["domain"]).sum()
)

# Print the stats
print(f"Initial Sources dataset duplicate count: {initial_duplicate_count}")
print(f"Newly processed dataset duplicate count: {new_duplicate_count}")
print(f"Merged dataset duplicate count: {merged_duplicate_count}")

# Match the datasets
processed_df.rename(columns={"source_name": "title", "rss_url": "rss"}, inplace=True)
processed_df["category"] = ""
processed_df.drop(
    columns=[
        "country",
        "usable_source",
        "is_scraping_allowed",
        "is_domain_up",
        "is_rss_feed_available",
        "is_rss_feed_valid",
    ],
    inplace=True,
)

# Remove duplicates before exporting
merged_df = pd.concat([initial_df, processed_df]).drop_duplicates(subset=["domain"])

# Print the final stats
print(f"Merged Sources dataset length: {len(merged_df)}")

# Export the merged dataset
merged_df.to_csv("./final_sources.csv", index=False)
