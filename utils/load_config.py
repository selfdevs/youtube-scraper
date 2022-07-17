from pathlib import Path
from dotenv import load_dotenv
from os import environ
from os.path import join


def load_keywords(filename):
    keywords = []
    with open(join(Path().cwd(), filename), "r") as file:
        for line in file.readlines():
            keywords.append(line.strip("\n"))

    return keywords[1:], keywords[0]


try:
    load_dotenv(join(Path().cwd(), ".env"))
    page_per_kw = int(environ["page_per_kw"])
    views_bar = int(environ["views_bar"])
    suggestions_language = environ["suggestions_language"]
    suggestions_region = environ["suggestions_region"]
    keywords_filename = environ["keywords_filename"]
    keywords, table_name = load_keywords(keywords_filename)
except Exception as e:
    print(f"Error occured while loading configurations: {e}")
