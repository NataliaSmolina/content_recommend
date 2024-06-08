# import numpy as np
# from sklearn.feature_extraction.text import TfidfVectorizer
# from weaviate import Client
# from sklearn.metrics.pairwise import cosine_similarity
# import pandas as pd
# import joblib
# from tqdm import tqdm


# client = Client("http://localhost:8080")
# try:

#     client.schema.delete_class("Games")

#     print("fine")
#     schema = client.schema.get()
#     print(schema)
# except Exception as ex:
#     print("ex: ", ex)
#     schema = client.schema.get()
#     print(schema)

# tfidf_class_schema_games = {
#     "class": "Games",
#     "description": "A collection of games",
#     "properties": [
#         {"name": "Title", "dataType": ["text"]},
#         {"name": "Release_Date", "dataType": ["number"]},
#         {"name": "Rating", "dataType": ["number"]},
#         {"name": "Genres", "dataType": ["text"]},
#         {"name": "Summary", "dataType": ["text"]},
#         {"name": "id_game", "dataType": ["number"]},
#         {"name": "link", "dataType": ["text"]},
#         {"name": "constraint", "dataType": ["number"]},
#         {"name": "CombinedVector", "dataType": ["number[]"]} 
#     ]
# }

# client.schema.create_class(tfidf_class_schema_games)


# # df_games = pd.read_csv('games.csv', nrows=int(pd.read_csv('games.csv').shape[0]*0.25))
# df_games = pd.read_csv('games.csv')

# df_games['Rating'] = 5
# df_games['Summary'] = df_games['Summary'].fillna('')
# df_games['Title'] = df_games['Title'].fillna('')
# df_games['Genres'] = df_games['Genres'].fillna('')
# df_games['Release_Date'] = df_games['Release_Date'].fillna(0)

# corpus_games = df_games['Title'] + " " + df_games['Summary'] + " " + df_games['Genres']


# vectorizer = TfidfVectorizer()
# X_games_combined = vectorizer.fit_transform(corpus_games)


# def add_objects_games(client, X_combined, df, collection_name):
#     for i in tqdm(range(len(df))):
#         object_data = {
#             'Title': str(df.iloc[i]['Title']),
#             'Release_Date': int(df.iloc[i]['Release_Date']),
#             'Rating': float(df.iloc[i]['Rating']),
#             'Genres': str(df.iloc[i]['Genres']),
#             'Summary': str(df.iloc[i]['Summary']),
#             'id_game': int(df.iloc[i]['id']),
#             'link': str(df.iloc[i]['link']),
#             'constraint': int(df.iloc[i]['Constraint']),
#             'CombinedVector': X_combined[i].toarray().tolist()[0]  # Вектор, объединяющий Title, Summary и Genres
#         }
#         try:
#             client.batch.add_data_object(object_data, collection_name)
#         except BaseException as error:
#             print("Import Failed at: ", i)
#             print("An exception occurred: {}".format(error))
#             break


# add_objects_games(client, X_games_combined, df_games, 'Games')


# client.batch.flush()

import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from weaviate import Client
from sklearn.metrics.pairwise import cosine_similarity
import pandas as pd
import joblib
from tqdm import tqdm


client = Client("http://localhost:8080")
try:
    client.schema.delete_class("Games")
    print("fine")
    schema = client.schema.get()
    print(schema)
except Exception as ex:
    print("ex: ", ex)
    schema = client.schema.get()
    print(schema)

tfidf_class_schema_games = {
    "class": "Games",
    "description": "A collection of games",
    "properties": [
        {"name": "Title", "dataType": ["text"]},
        {"name": "Release_Date", "dataType": ["number"]},
        {"name": "Rating", "dataType": ["number"]},
        {"name": "Genres", "dataType": ["text"]},
        {"name": "Summary", "dataType": ["text"]},
        {"name": "id_game", "dataType": ["number"]},
        {"name": "link", "dataType": ["text"]},
        {"name": "constraint", "dataType": ["number"]},
        {"name": "CombinedVector", "dataType": ["number[]"], "index": True}  # Индексация CombinedVector
    ]
}

client.schema.create_class(tfidf_class_schema_games)


# df_games = pd.read_csv('games.csv', nrows=int(pd.read_csv('games.csv').shape[0]*0.25))
df_games = pd.read_csv('games.csv')

df_games['Rating'] = 5
df_games['Summary'] = df_games['Summary'].fillna('')
df_games['Title'] = df_games['Title'].fillna('')
df_games['Genres'] = df_games['Genres'].fillna('')
df_games['Release_Date'] = df_games['Release_Date'].fillna(0)

corpus_games = df_games['Title'] + " " + df_games['Summary'] + " " + df_games['Genres']


vectorizer = TfidfVectorizer()
X_games_combined = vectorizer.fit_transform(corpus_games)


def add_objects_games(client, X_combined, df, collection_name):
    for i in tqdm(range(len(df))):
        object_data = {
            'Title': str(df.iloc[i]['Title']),
            'Release_Date': int(df.iloc[i]['Release_Date']),
            'Rating': float(df.iloc[i]['Rating']),
            'Genres': str(df.iloc[i]['Genres']),
            'Summary': str(df.iloc[i]['Summary']),
            'id_game': int(df.iloc[i]['id']),
            'link': str(df.iloc[i]['link']),
            'constraint': int(df.iloc[i]['Constraint']),
            'CombinedVector': X_combined[i].toarray().tolist()[0]  # Вектор, объединяющий Title, Summary и Genres
        }
        try:
            client.batch.add_data_object(object_data, collection_name)
        except BaseException as error:
            print("Import Failed at: ", i)
            print("An exception occurred: {}".format(error))
            break


add_objects_games(client, X_games_combined, df_games, 'Games')


client.batch.flush()
