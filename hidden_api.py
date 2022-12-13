"""
Scrapes product data from Red Dragon's e-commerce website on Shopify.

* Should create an error if the response code is not correct.
    - Could have a skip option argument.

Author: Data-Dylan
"""

# Software packages.
import pandas as pd
from collections import defaultdict
from requests import get
from scrapy.selector import Selector
from time import sleep

# Constants.
sitemap_uri = "https://reddragon.ca/sitemap.xml"
sleep_timer = 2

# Dataframe list to append data to.
df_list = []

# Get XML sitemap in text form.
sitemap = get(sitemap_uri).text
sleep(sleep_timer)

# Get the product sitemap.
sitemap_children = Selector(text = sitemap).xpath("//loc/text()").getall()
product_sitemap_uri = filter(lambda url: "products" in url, sitemap_children)
product_sitemap_uri = list(product_sitemap_uri)
if len(product_sitemap_uri) == 1:
    product_sitemap_uri = product_sitemap_uri[0]
else:
    raise Exception("Unexpected sitemap result. Expected to filter down " + \
                    "to a list of length 1. Returned result has a length " + \
                    f" of {len(product_sitemap_uri)}.") 

# Get XML product sitemap in text form.
product_sitemap = get(product_sitemap_uri).text
sleep(sleep_timer)

# List of product hyperlinks in the sitemap.
product_uris = Selector(text = product_sitemap).xpath("//loc/text()").getall()
product_uris = filter(lambda uri: r"reddragon.ca/products/" in uri, product_uris)

# Add the JSON data-variant of the product page URI
product_uris = list(map(lambda uri: uri + ".json", product_uris))

# Iterate through product URIs
for i, uri in enumerate(product_uris):
    
    # Reset data dictionary.
    data = defaultdict(list)
    
    # Get the data in its raw json format as a python dictionary.
    raw_data = get(uri).json()["product"]
    
    # Delete unwanted redundant data.
    del raw_data["id"]
    del raw_data["images"]
    del raw_data["options"]
    
    # Create list of keys without the variants key included.
    main_keys = list(raw_data.keys())
    main_keys.remove("variants")
    
    # Iterate through product variants.
    for variant in raw_data["variants"]:
        
        # Iterate through keys that do not include the product variant key.
        for main_key in main_keys:
            
            # Add main product features to the variant product data.
            if main_key == "image":
                if raw_data[main_key] == None:
                    data[main_key] = raw_data[main_key]
                else:
                    data[main_key] = raw_data[main_key]["src"]
            else:
                data[main_key] = raw_data[main_key]
            
        # Add the variant-specific product data.
        for variant_key in variant.keys():
            data["variant_" + variant_key].append(variant[variant_key])
    
    # Append data to dataframe list.
    df_list.append(pd.DataFrame(data))
    
    # Iteration update message.
    print(f"{i + 1}/{len(product_uris)} products scraped.")
    sleep(sleep_timer)
            
# Aggregate list of dataframes into one big data frame.
aggregate_df = pd.concat(df_list)

# Export data as a CSV.
aggregate_df.to_csv("./raw_data.csv", index = None)
print("Done.")





