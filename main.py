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
from multiprocessing import Pool
from multiprocessing.pool import ThreadPool

db_conn = db_helper.create_connection("2.db", table_name)
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
searched_keywords = set()
suggestions = Suggestions(region=suggestions_region, language=suggestions_language)


def get_results(kw):
    global kw_list
    search = VideosSearch(kw)

    print(f"keywords: {keywords}")
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
                        dict_for_df["thumbnails"].append(data["thumbnails"][0]["url"])
                        dict_for_df["video_id"].append(data["id"])

        data = search.next()

    pd.DataFrame.from_dict(dict_for_df).to_sql(
        name=table_name,
        if_exists="append",
        con=db_conn,
        index=False,
    )
    searched_keywords.add(kw)
    keywords.pop(0)
    suggested = set(suggestions.get(kw)["result"])
    for suggested in suggested:
        if suggested not in searched_keywords:
            print(f"suggested from {kw}: {suggested}")
            keywords.append(suggested)
    pool = ThreadPool(6)
    pool.map(get_results, keywords)
    pool.close()
    pool.join()


if __name__ == "__main__":
    try:
        print(table_name)
        pool = Pool(6)
        pool.map(get_results, keywords)
        pool.close()
        pool.join()
    except Exception as e:
        print(f"Exception occured while executing main {e}")
