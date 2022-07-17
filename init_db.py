from sqlite3 import Error
import sqlite3
import traceback


def create_connection(local_db_path, table_name):

    conn = sqlite3.connect(local_db_path, check_same_thread=False)
    # # logging.info(f"SQLite DB has been created with version {sqlite3.version}")

    # # mycursor = conn.cursor()
    # # tables = mycursor.execute("TABLES")
    # # print(type(tables))
    # cursor = conn.cursor()
    # # try:
    # #     cursor.execute(
    # #         f'SELECT * FROM SQLITE_SCHEMA WHERE type="table" AND TABLE_NAME="{table_name}"'
    # #     ).fetchall()
    # # except sqlite3.OperationalError as e:
    # cursor.execute(
    #     f"CREATE TABLE IF NOT EXISTS {table_name} (video_id VARCHAR(255) PRIMARY KEY, view_count BIGINT(255), title VARCHAR(255), pub_time VARCHAR(255), duration VARCHAR(255), channel_name VARCHAR(255), channel_id VARCHAR(255), channel_link VARCHAR(255), thumbnails VARCHAR(255))"
    # )

    # print(f"Connected to {table_name}")
    return conn


def create_db(conn, config_data):
    try:
        print("Creating db", config_data["queries"]["create_maintable"])
        conn.execute(config_data["queries"]["create_maintable"])
    except Exception as e:
        print(f"Error occured while creating DB: {e}")


## alternative way to insert without using DataFrames
def insert_into_db(df, conn):
    cols = "`,`".join([str(i) for i in df.columns.tolist()])
    cur = conn.cursor()

    try:
        for i, row in df.iterrows():
            sql = (
                "INSERT INTO `youtube_videos` (`"
                + cols
                + "`) VALUES ("
                + "%s," * (len(row) - 1)
                + "%s)"
            )
            cur.execute(sql, tuple(row))
        conn.commit()
    except Exception:
        print(traceback.format_exc())


# just a fct to connect to db
def check_db():
    con = sqlite3.connect("youtube_videos.db")
    cur = con.cursor()

    for row in cur.execute("SELECT * FROM youtube_videos1;"):
        print(row)

    con.close()
