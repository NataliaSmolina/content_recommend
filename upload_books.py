# import numpy as np
# from sklearn.feature_extraction.text import TfidfVectorizer
# from weaviate import Client
# from sklearn.metrics.pairwise import cosine_similarity
# import pandas as pd

# from tqdm import tqdm


# client = Client("http://localhost:8080")
# try:

#     client.schema.delete_class("Books")
#     print("fine")
#     schema = client.schema.get()
#     print(schema)
# except Exception as ex:
#     print("ex: ", ex)
#     schema = client.schema.get()
#     print(schema)
# #  book_title,author,publication_date,summary,id,link,Constraint
# tfidf_class_schema_books = {
#     "class": "Books",
#     "description": "A collection of books",
#     "properties": [
#         {"name": "book_title", "dataType": ["text"]},
#         {"name": "author", "dataType": ["text"]},
#         {"name": "publication_date", "dataType": ["number"]},
#         {"name": "summary", "dataType": ["text"]},
#         {"name": "id_book", "dataType": ["number"]},
#         {"name": "link", "dataType": ["text"]},
#         {"name": "constraint", "dataType": ["number"]},
#         {"name": "CombinedVector", "dataType": ["number[]"]}  
#     ]
# }


# client.schema.create_class(tfidf_class_schema_books)


# # df_books = pd.read_csv('books.csv',nrows=int(pd.read_csv('books.csv').shape[0]*0.25))
# df_books = pd.read_csv('books.csv')

# df_books['summary'] = df_books['summary'].fillna('')
# df_books['book_title'] = df_books['book_title'].fillna('')
# df_books['author'] = df_books['author'].fillna('')
# df_books['publication_date'] = df_books['publication_date'].fillna(0).astype(int)


# corpus_books = df_books['book_title'] + " " + df_books['summary'] + " " + df_books['author']

# vectorizer = TfidfVectorizer()
# X_books_combined = vectorizer.fit_transform(corpus_books)


# def add_objects_books(client, X_combined, df, collection_name):
#     for i in tqdm(range(len(df))):
#         object_data = {
#             'book_title': str(df.iloc[i]['book_title']),
#             'author': str(df.iloc[i]['author']),
#             'publication_date': int(df.iloc[i]['publication_date']) if not pd.isnull(df.iloc[i]['publication_date']) else 0,
#             'summary': str(df.iloc[i]['summary']),
#             'id_book': int(df.iloc[i]['id']),
#             'link': str(df.iloc[i]['link']),
#             'constraint': int(df.iloc[i]['Constraint']),
#             'CombinedVector': X_combined[i].toarray().tolist()[0]  
#         }
#         try:
#             client.batch.add_data_object(object_data, collection_name)
#         except BaseException as error:
#             print("Import Failed at: ", i)
#             print("An exception occurred: {}".format(error))
#             break


# add_objects_books(client, X_books_combined, df_books, 'Books')


# client.batch.flush()

import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from weaviate import Client
import pandas as pd
from tqdm import tqdm

client = Client("http://localhost:8080")

try:
    client.schema.delete_class("Books")
    print("fine")
    schema = client.schema.get()
    print(schema)
except Exception as ex:
    print("ex: ", ex)
    schema = client.schema.get()
    print(schema)

# Схема для класса Books
tfidf_class_schema_books = {
    "class": "Books",
    "description": "A collection of books",
    "properties": [
        {"name": "book_title", "dataType": ["text"]},
        {"name": "author", "dataType": ["text"]},
        {"name": "publication_date", "dataType": ["number"]},
        {"name": "summary", "dataType": ["text"]},
        {"name": "id_book", "dataType": ["number"]},
        {"name": "link", "dataType": ["text"]},
        {"name": "constraint", "dataType": ["number"]},
        {"name": "CombinedVector", "dataType": ["number[]"]}  
    ]
}

client.schema.create_class(tfidf_class_schema_books)

# Загрузка данных из CSV файла
df_books = pd.read_csv('books.csv')
df_books['summary'] = df_books['summary'].fillna('')
df_books['book_title'] = df_books['book_title'].fillna('')
df_books['author'] = df_books['author'].fillna('')
df_books['publication_date'] = df_books['publication_date'].fillna(0).astype(int)

# Объединение текстовых данных в один вектор
corpus_books = df_books['book_title'] + " " + df_books['summary'] + " " + df_books['author']
vectorizer = TfidfVectorizer()
X_books_combined = vectorizer.fit_transform(corpus_books)

# Добавление объектов в Weaviate
def add_objects_books(client, X_combined, df, collection_name):
    for i in tqdm(range(len(df))):
        object_data = {
            'book_title': str(df.iloc[i]['book_title']),
            'author': str(df.iloc[i]['author']),
            'publication_date': int(df.iloc[i]['publication_date']) if not pd.isnull(df.iloc[i]['publication_date']) else 0,
            'summary': str(df.iloc[i]['summary']),
            'id_book': int(df.iloc[i]['id']),
            'link': str(df.iloc[i]['link']),
            'constraint': int(df.iloc[i]['Constraint']),
            'CombinedVector': X_combined[i].toarray().tolist()[0]  
        }
        try:
            client.batch.add_data_object(object_data, collection_name)
        except BaseException as error:
            print("Import Failed at: ", i)
            print("An exception occurred: {}".format(error))
            break

add_objects_books(client, X_books_combined, df_books, 'Books')
client.batch.flush()

