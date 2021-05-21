from facepy import GraphAPI
import time
import pandas as pd
import datetime
from sqlalchemy import create_engine
import psycopg2


def reader(name):
    with open(name, 'r') as file:
        return file.read()


Secret = reader('app_secret.txt')
Token = reader('app_token.txt')
graph = GraphAPI(Token)
Engine = create_engine("postgresql+psycopg2://postgres:{}@localhost/fb_db".format(reader("postpas.txt")))
con = psycopg2.connect(database="fb_db", user='postgres', password=reader("postpas.txt"), host="localhost",
                       port=5432)

# получаем данные о группах, в которых пользователь администратор
groups = [[group['id'], group['name'], group['picture']['data']['url']] for group in
          graph.get('me/groups?fields=administrator,picture,name')['data'] if group['administrator'] == True]
pd.DataFrame(groups, columns=['group_id', 'group_name', 'group_picture']).to_sql(
    'groups_table', con=Engine, if_exists='replace', index=False)
# получаем данные о постах групп
postst_list = []
photos = []
for group in groups:
    info = graph.get('{}/feed?fields=message,attachments'.format(group[0]))['data']
    postst_list += [[post['id'], post.get("message", ' '), group[0]] for post in info]

    for post in info:
        if 'attachments' in post.keys():
            for image in post['attachments']['data']:
                if 'subattachments' in image.keys():
                    photos += [[imag['media']['image']['src'], post['id'], group[0]] for imag in
                               image['subattachments']['data']]
                else:
                    photos += [[image['media']['image']['src'], post['id'], group[0]]]

try:
    cur = con.cursor()
    cur.execute("SELECT * FROM groups_posts_table")
    f = cur.fetchall()
except Exception:
    f = False
if not f or f == []:
    pd.DataFrame(postst_list, columns=['post_id', 'message', 'group_id']).to_sql(
        'groups_posts_table', con=Engine, if_exists='append', index=False)
else:
    update_posts_list = []
    update_posts_list += postst_list
    for post in f:
        if list(post) not in postst_list:
            update_posts_list.append(list(post))

    con.close()
    pd.DataFrame(update_posts_list, columns=['post_id', 'message', 'group_id']).to_sql(
        'groups_posts_table', con=Engine, if_exists='replace', index=False)

try:
    con = psycopg2.connect(database="fb_db", user='postgres', password=reader("postpas.txt"), host="localhost",
                           port=5432)
    cur = con.cursor()
    cur.execute("SELECT * FROM groups_posts_photos")
    f = cur.fetchall()
except Exception:
    f = False
if not f or f == []:
    pd.DataFrame(photos, columns=['photo', 'post_id', 'group_id']).to_sql(
        'groups_posts_photos', con=Engine, if_exists='append', index=False)
else:

    update_photos_list = []
    update_photos_list += photos
    for photo in f:

        if list(photo) not in photos:

            update_photos_list.append(list(photo))

    con.close()
    pd.DataFrame(update_photos_list, columns=['photo', 'post_id', 'group_id']).to_sql(
        'groups_posts_photos', con=Engine, if_exists='replace', index=False)


