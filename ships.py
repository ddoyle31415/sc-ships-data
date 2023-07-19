import re
import os
import shutil
from collections import OrderedDict

# import utils

import pandas as pd
import tqdm
import argparse
import requests
import bs4

STAR_CITIZEN_WIKI_ROOT = "https://starcitizen.tools"


def load_page(url):
    page = requests.get(url)
    return bs4.BeautifulSoup(page.content, "html.parser")


def get_in_space_image_src(ship_page):
    """
    Get space image sources

    Not all pages have the right image table
    if the "Ship Profile" header isn't found, return None
    Otherwise return the list of image sources
    """
    ship_profile_header = ship_page.find("span", id="Ship_profile")

    if not ship_profile_header:
        return None

    def find_table_wrap(tag):
        return (
            tag.name == "div"
            and tag.has_attr("class")
            and (
                "tabber" in tag.attrs["class"]
                or "citizen-table-wrapper" in tag.attrs["class"]
            )
        )

    try:
        img_table = ship_profile_header.parent.find_next_sibling(find_table_wrap).find(
            "article", attrs={"data-title": re.compile("In[\s|-]space")}
        )
    except AttributeError:
        img_table = None

    srcs = []
    if img_table:
        figure_elements = img_table.find_all(
            "figure", attrs={"typeof": "mw:File/Frameless"}
        )

        # for element in ship_page.find("table", "wikitable").find_all(
        for element in figure_elements:
            media_page_url = STAR_CITIZEN_WIKI_ROOT + element.find("a")["href"]
            media_page = load_page(media_page_url)
            srcs.append(media_page.find("a", "internal")["href"])
    return srcs


def download_image(url, file_name):
    response = requests.get(url, stream=True)
    with open(file_name, "wb") as f:
        shutil.copyfileobj(response.raw, f)
    del response


# extract_cell_VAR functions extract cell values from the HTML ships table
def extract_cell_Name(row_element):
    return row_element.find("td", "Name").a["title"]


def extract_cell_Wiki(row_element):
    return STAR_CITIZEN_WIKI_ROOT + row_element.find("td", "Name").a["href"]


def extract_cell_Manufacturer(row_element):
    return row_element.find("td", "Manufacturer").a["title"]


def extract_cell_Size(row_element):
    return row_element.find("td", "Size").contents[0]


def extract_cell_Length(row_element):
    return float(row_element.find("td", "Length")["data-sort-value"])


def extract_cell_Width(row_element):
    return float(row_element.find("td", "Width")["data-sort-value"])


def extract_cell_Height(row_element):
    return float(row_element.find("td", "Height")["data-sort-value"])


def extract_cell_Max_speed(row_element):
    try:
        return float(row_element.find("td", "Max-speed")["data-sort-value"])
    except KeyError:
        return -1


def extract_cell_SCM_speed(row_element):
    try:
        return float(row_element.find("td", "SCM-speed")["data-sort-value"])
    except KeyError:
        return -1


def extract_cell_0_SCM_time(row_element):
    try:
        return float(row_element.find("td", "0-SCM-time")["data-sort-value"])
    except KeyError:
        return -1


def extract_row(row_element, columns):
    """
    Loop over all of the cell extraction functions in `columns` to produce data corresponding to one ship
    """
    data = []
    for column in columns:
        data.append(columns[column](row_element))
    return data


def extract_rows(ships_table, columns):
    """
    Extract all data from the HTML ships table
    """
    even_rows = [
        extract_row(row_element, columns)
        for row_element in ships_table.find_all("tr", "row-even")
    ]
    odd_rows = [
        extract_row(row_element, columns)
        for row_element in ships_table.find_all("tr", "row-odd")
    ]

    all_rows = [None] * (len(even_rows) + len(odd_rows))
    all_rows[::2] = odd_rows
    all_rows[1::2] = even_rows
    return all_rows


# Description of extracted table
# dict keys are column names and values are filled with
# the corresponding extract_cell_VAR functions
SHIPS_TABLES_DEF = OrderedDict(
    {
        "Name": extract_cell_Name,
        "Wiki": extract_cell_Wiki,
        "Manufacturer": extract_cell_Manufacturer,
        "Size": extract_cell_Size,
        "Length (m)": extract_cell_Length,
        "Width (m)": extract_cell_Width,
        "Height (m)": extract_cell_Height,
        "Max speed (m/s)": extract_cell_Max_speed,
        "SCM speed (m/s)": extract_cell_SCM_speed,
        "0-SCM time (s)": extract_cell_0_SCM_time,
    }
)

# Where to find the ships table
SHIPS_TABLES_URL = "https://starcitizen.tools/List_of_pledge_vehicles"


def load_ships_table(ships_page):
    """
    Load the ships table HTML element
    """
    return ships_page.find("table", "template-pledgevehiclelist")


def get_ships_data():
    """
    Top level function for scraping ship data into a dataframe format
    """
    ships_table = load_ships_table(load_page(SHIPS_TABLES_URL))
    df = pd.DataFrame(
        extract_rows(ships_table, SHIPS_TABLES_DEF),
        columns=list(SHIPS_TABLES_DEF.keys()),
    )
    df = df.set_index("Name")
    return df


def view_fault_tolerance(view):
    """
    Tom-hackery to correct typos server-side
    """
    if view == "Isometirc":
        return "Isometric"
    if view == "Isometrric":
        return "Isometric"
    return view


def download_images(ships_data, destination, overwrite=False):
    """
    Top level function for downloading images based on ship data and
    returning an image database
    """
    destination = "data/images"
    os.makedirs(destination, exist_ok=True)
    n_ships = ships_data.shape[0]
    img_data = {
        "Name": [None] * n_ships,
        "Isometric": [None] * n_ships,
        "Above": [None] * n_ships,
        "Port": [None] * n_ships,
        "Front": [None] * n_ships,
        "Rear": [None] * n_ships,
        "Below": [None] * n_ships,
    }

    pattern = re.compile("([A-Za-z])+(?=\.jpg|\.png)")
    with tqdm.tqdm(total=ships_data.shape[0]) as pbar:
        for idx, (name, ship) in enumerate(ships_data.iterrows()):
            img_data["Name"][idx] = name

            try:
                img_srcs = get_in_space_image_src(load_page(ship["Wiki"]))
            except:
                print("Problem with", ship["Wiki"])
                raise

            # if images were found on the page
            if img_srcs:
                img_paths = [
                    os.path.join(destination, os.path.basename(img_src))
                    for img_src in img_srcs
                ]

                for img_path, img_src in zip(img_paths, img_srcs):
                    view_match = pattern.search(img_path)
                    # if images match expected pattern, download. Otherwise throw away
                    if view_match:
                        view = view_fault_tolerance(view_match[0])

                        try:
                            img_data[view][idx] = os.path.basename(img_path)
                        except KeyError:
                            print(name, view_match[0], img_srcs)
                            raise
                        if overwrite or not os.path.isfile(img_path):
                            download_image(img_src, img_path)

            pbar.update()

    df = pd.DataFrame(img_data)
    df = df.set_index("Name")
    return df


# Run this module as a script to produce the dataset
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("destination")
    parser.add_argument("--overwrite")
    args = parser.parse_args()

    ships_data = get_ships_data()
    img_data = download_images(
        ships_data, os.path.join(args.destination, "images"), args.overwrite
    )

    ships_data.to_csv(os.path.join(args.destination, "ships_data.csv"))
    img_data.to_csv(os.path.join(args.destination, "img_data.csv"))
