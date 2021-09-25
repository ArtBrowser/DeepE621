import json
import os
import sqlite3


def load_tags(tags_path):
    with open(tags_path, 'r', encoding='utf-8') as tags_stream:
        tags = [tag for tag in (tag.strip() for tag in tags_stream) if tag]
        return tags


def load_image_records(sqlite_path, minimum_tag_count):
    print("Loading image records with " + str(minimum_tag_count) + " tags.")
    cache_path = os.path.join(os.path.dirname(sqlite_path), 'data.txt')
    if os.path.exists(cache_path):
        image_records = []
        with open(cache_path, 'r') as json_file:
            data = json.load(json_file)
            for row in data:
                image_records.append((row[0], row[1]))
        return image_records


    if not os.path.exists(sqlite_path):
        raise Exception(f'SQLite database is not exists : {sqlite_path}')

    connection = sqlite3.connect(sqlite_path)
    connection.row_factory = sqlite3.Row
    cursor = connection.cursor()

    image_folder_path = os.path.join(os.path.dirname(sqlite_path), 'images')

    print("Querying db...")
    cursor.execute(
        "SELECT file_url, md5, file_ext, tag_string FROM posts WHERE (file_ext = 'png' OR file_ext = 'jpg' OR file_ext = 'jpeg') AND (tag_count_general >= ?) ORDER BY id",
        (minimum_tag_count,))

    rows = cursor.fetchall()

    image_records = []

    for row in rows:
        md5 = row['md5']
        extension = row['file_ext']
        image_path = os.path.join(
            image_folder_path, md5[0:2], f'{md5}.{extension}')
        file_url = row['file_url']
        tag_string = row['tag_string']

        image_records.append((file_url, tag_string))

    connection.close()

    return image_records
