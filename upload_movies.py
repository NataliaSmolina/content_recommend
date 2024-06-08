

# import pandas as pd
# from sklearn.feature_extraction.text import TfidfVectorizer
# from weaviate import Client
# from tqdm import tqdm


# client = Client("http://localhost:8080")



# try:
#     client.schema.delete_class("Movies")
#     print("fine")
#     schema = client.schema.get()
#     print(schema)
# except Exception as ex:
#     print("ex: ", ex)
#     schema = client.schema.get()
#     print(schema)


# tfidf_class_schema_movies = {
#     "class": "Movies",
#     "description": "A collection of movies",
#     "properties": [
#         {"name": "PosterLink", "dataType": ["text"]},
#         {"name": "Description", "dataType": ["text"]},
#         {"name": "year", "dataType": ["number"]},
#         {"name": "Title", "dataType": ["text"]},
#         {"name": "Genre", "dataType": ["text"]},
#         {"name": "Plot", "dataType": ["text"]},
#         {"name": "Constraint", "dataType": ["number"]},
#         {"name": "id_film", "dataType": ["number"]},
#         {"name": "link", "dataType": ["text"]},
#         {"name": "CombinedVector", "dataType": ["number[]"]} 
#     ]
# }


# client.schema.create_class(tfidf_class_schema_movies)


# # df_movies = pd.read_csv('films2.csv', nrows=int(pd.read_csv('films2.csv').shape[0]*0.25))
# df_movies = pd.read_csv('films2.csv')

# df_movies['Description'] = df_movies['Description'].fillna('')
# df_movies['Title'] = df_movies['Title'].fillna('')
# df_movies['Plot'] = df_movies['Plot'].fillna('')


# corpus_movies = df_movies['Title'] + " " + df_movies['Description'] + " " + df_movies['Plot']


# vectorizer = TfidfVectorizer()
# X_movies_combined = vectorizer.fit_transform(corpus_movies)


# def add_objects_movies(client, X_combined, df, collection_name):
#     for i in tqdm(range(len(df))):
#         object_data = {
#             'PosterLink': str(df_movies.iloc[i]['PosterLink']),
#             'Description': str(df_movies.iloc[i]['Description']),
#             'year': int(df_movies.iloc[i]['year']),
#             'Title': str(df_movies.iloc[i]['Title']),
#             'Genre': str(df_movies.iloc[i]['Genre']),
#             'Plot': str(df_movies.iloc[i]['Plot']),
#             'Constraint': int(df_movies.iloc[i]['Constraint']),
#             'id_film': int(df_movies.iloc[i]['id']),
#             'link': str(df_movies.iloc[i]['link']),
#             'CombinedVector': X_combined[i].toarray().tolist()[0] 
#         }
#         try:
#             client.batch.add_data_object(object_data, collection_name)
#         except BaseException as error:
#             print("Import Failed at: ", i)
#             print("An exception occurred: {}".format(error))
#             break


# add_objects_movies(client, X_movies_combined, df_movies, 'Movies')


# client.batch.flush()

import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from weaviate import Client
from tqdm import tqdm

client = Client("http://localhost:8080")

try:
    client.schema.delete_class("Movies")
    print("Schema for Movies deleted successfully")
except Exception as ex:
    print("Exception occurred: ", ex)

# Определение схемы для класса Movies
tfidf_class_schema_movies = {
    "class": "Movies",
    "description": "A collection of movies",
    "properties": [
        {"name": "PosterLink", "dataType": ["text"]},
        {"name": "Description", "dataType": ["text"]},
        {"name": "year", "dataType": ["number"]},
        {"name": "Title", "dataType": ["text"]},
        {"name": "Genre", "dataType": ["text"]},
        {"name": "Plot", "dataType": ["text"]},
        {"name": "Constraint", "dataType": ["number"]},
        {"name": "id_film", "dataType": ["number"]},
        {"name": "link", "dataType": ["text"]},
        {"name": "CombinedVector", "dataType": ["number[]"], "index": True} 
    ]
}

client.schema.create_class(tfidf_class_schema_movies)

# Загрузка данных из CSV файла
df_movies = pd.read_csv('films2.csv')

df_movies['Description'] = df_movies['Description'].fillna('')
df_movies['Title'] = df_movies['Title'].fillna('')
df_movies['Plot'] = df_movies['Plot'].fillna('')

# Объединение текстовых данных в один вектор
corpus_movies = df_movies['Title'] + " " + df_movies['Description'] + " " + df_movies['Plot']

vectorizer = TfidfVectorizer()
X_movies_combined = vectorizer.fit_transform(corpus_movies)

# Добавление объектов в Weaviate
def add_objects_movies(client, X_combined, df, collection_name):
    for i in tqdm(range(len(df))):
        object_data = {
            'PosterLink': str(df.iloc[i]['PosterLink']),
            'Description': str(df.iloc[i]['Description']),
            'year': int(df.iloc[i]['year']),
            'Title': str(df.iloc[i]['Title']),
            'Genre': str(df.iloc[i]['Genre']),
            'Plot': str(df.iloc[i]['Plot']),
            'Constraint': int(df.iloc[i]['Constraint']),
            'id_film': int(df.iloc[i]['id']),
            'link': str(df.iloc[i]['link']),
            'CombinedVector': X_combined[i].toarray().tolist()[0] 
        }
        try:
            client.batch.add_data_object(object_data, collection_name)
        except BaseException as error:
            print("Import Failed at: ", i)
            print("An exception occurred: {}".format(error))
            break

add_objects_movies(client, X_movies_combined, df_movies, 'Movies')
client.batch.flush()


