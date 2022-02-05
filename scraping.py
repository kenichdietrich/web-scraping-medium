import requests
from bs4 import BeautifulSoup
import pandas as pd
import calendar
import datetime
import time
import os

DATA_FILE = "towards_data_science_2020.csv"

def generate_days(year):
    """Generates all tuples (YYYY, MM, DD) of days in a year
    """
    cal = calendar.Calendar()
    days = []
    for m in range(1,13):
        days.extend(list(cal.itermonthdays3(year, m)))
    days = [d for d in set(days) if d[0] == year]
    days.sort()

    return days

def generate_url(year, month, day):
    """Generates the dynamic Medium url to be scraped
    """
    return "https://towardsdatascience.com/archive/{}/{:0>2}/{:0>2}".format(year, month, day)

def get_tag_text(tag):
    """Gets the leaf text of a BS node if it exists (else, returns empty string)
    """
    try:
        text = tag.text
    except:
        text = ""
    return text

def extract_post_data(card):
    """Extracts all relevant data from a post web card.
    """
    card_divs = card.find_all("div", recursive=False)

    user_data = card_divs[0].find("div", class_="postMetaInline-authorLockup")
    user = {}
    user["user_id"] = user_data.a["data-user-id"]
    user["user_name"] = user_data.a.string
    user["user_href"] = user_data.a["href"].split("?")[0]

    post_data = card_divs[1].find(class_="section-inner")
    post = {}
    post["post_id"] = card["data-post-id"]
    figure = post_data.figure
    post["post_image"] = figure.find(class_="progressiveMedia-image")["data-src"] if (figure and figure.find(class_="progressiveMedia-image")) else ""
    post["post_title"] = get_tag_text(post_data.find(class_="graf--title")).replace(u'\xa0', u' ')
    subtitle = post_data.find(class_="graf--subtitle")
    post["post_subtitle"] = get_tag_text(subtitle).replace(u'\xa0', u' ') if subtitle else ""
    suptitle = post_data.find(class_="graf--kicker")
    post["post_suptitle"] = get_tag_text(suptitle).replace(u'\xa0', u' ') if suptitle else ""
    post["post_href"] = card_divs[1].a["href"].split("?")[0]
    post["post_claps"] = get_tag_text(card_divs[3].find(class_="js-multirecommendCountButton"))
    responses = card_divs[3].find("div", class_="buttonSet").a
    post["post_responses"] = responses.string if responses else "0"
    post["post_reading_time"] = user_data.find(class_="readingTime")["title"].replace("read", "").strip()

    return {**user, **post}

HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}

# Generates all tuples of days in a year
days = generate_days(2020)

# Filtering days
days = [d for d in days if (d[1] in (6,7,8,9,10)) or ((d[1] == 11) and (d[2] < 6))]

posts = []
# Web scraping loop
for d in days:
    r = requests.get(generate_url(*d), headers=HEADERS)
    print("{}/{:0>2}/{:0>2}: request status {}, elapsed_time: {}".format(d[0], d[1], d[2], r.status_code, r.elapsed))

    soup = BeautifulSoup(r.text, "lxml")
    cards = soup.find_all("div", class_="postArticle")
    print("{} posts found".format(len(cards)))
    day_posts = []
    for card in cards:
        post = extract_post_data(card)
        post["post_year"] = d[0]
        post["post_month"] = d[1]
        post["post_day"] = d[2]
        post["meta_timestamp"] = datetime.datetime.utcnow()
        day_posts.append(post)
    posts.extend(day_posts)
    pd.DataFrame(posts).to_csv(os.path.join("data", DATA_FILE), sep=";", index=False, quoting=2)
    print("CSV saved up to {}/{:0>2}/{:0>2}".format(d[0], d[1], d[2]))

    time.sleep(1)
