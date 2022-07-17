import re
from youtubesearchpython import VideosSearch, Suggestions
from utils.load_config import (
    page_per_kw,
    views_bar,
    keywords,
    suggestions_language,
    suggestions_region,
    table_name,
)
import pandas as pd
import init_db as db_helper

dict_for_df = {
    "view_count": [],
    "title": [],
    "video_id": [],
    "pub_time": [],
    "duration": [],
    "channel_name": [],
    "channel_id": [],
    "channel_link": [],
    "thumbnails": [],
}

json_file = open("test.json", "w")


def get_results(kw):
    search = VideosSearch(kw)
    try:
        print(kw)
        for page in range(0, page_per_kw):
            for i in range(0, len(search.result()["result"])):
                data = search.result()["result"][i]
                if data["viewCount"]["text"] and "views" in data["viewCount"]["text"]:
                    if (
                        data["viewCount"]["text"] != "No views"
                        or data["viewCount"]["text"] != ""
                        or data["viewCount"]["text"] is not None
                    ):
                        views = int(re.sub("[^0-9]", "", data["viewCount"]["text"]))
                        if views >= views_bar:
                            dict_for_df["view_count"].append(views)
                            dict_for_df["title"].append(data["title"])
                            dict_for_df["channel_id"].append(data["channel"]["id"])
                            dict_for_df["channel_link"].append(data["channel"]["link"])
                            dict_for_df["channel_name"].append(data["channel"]["name"])

                            dict_for_df["duration"].append(data["duration"])
                            dict_for_df["pub_time"].append(data["publishedTime"])
                            dict_for_df["thumbnails"].append(
                                data["thumbnails"][0]["url"]
                            )
                            dict_for_df["video_id"].append(data["id"])
    except Exception as e:
        print(views)

        data = search.next()


def main():
    db_conn = db_helper.create_connection("yt.db")
    searched_keywords = set()
    suggestions = Suggestions(region=suggestions_region, language=suggestions_language)
    print(keywords)
    try:
        for keyword in keywords:
            if keyword not in searched_keywords:
                get_results(keyword)
                pd.DataFrame.from_dict(dict_for_df).to_sql(
                    name={table_name}, if_exists="append", con=db_conn, index=False
                )
                searched_keywords.add(keyword)
                suggested = set(suggestions.get(keyword)["result"])
                for kw in suggested:
                    if kw not in searched_keywords:
                        keywords.append(kw)
    except Exception as e:
        print(f"keyword: {keyword}")
        print(e)


main()
