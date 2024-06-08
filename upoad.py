import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from weaviate import Client
from sklearn.metrics.pairwise import cosine_similarity
import pandas as pd
from tqdm import tqdm

def initialize_client():
    client = Client("http://localhost:8080")
    try:
        client.schema.delete_class("Movies")
        client.schema.delete_class("Games")
        client.schema.delete_class("Books")
        print("fine")
        schema = client.schema.get()
        print(schema)
    except Exception as ex:
        print("ex: ", ex)
        schema = client.schema.get()
        print(schema)
    return client

def create_books_schema(client):
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

def process_books_data():
    df_books = pd.read_csv('books.csv', nrows=int(pd.read_csv('books.csv').shape[0]*0.25))
    df_books['summary'] = df_books['summary'].fillna('')
    df_books['book_title'] = df_books['book_title'].fillna('')
    df_books['author'] = df_books['author'].fillna('')
    df_books['publication_date'] = df_books['publication_date'].fillna(0).astype(int)
    corpus_books = df_books['book_title'] + " " + df_books['summary'] + " " + df_books['author']
    vectorizer = TfidfVectorizer()
    X_books_combined = vectorizer.fit_transform(corpus_books)
    return X_books_combined, df_books

def add_objects_books(client, X_combined, df):
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
            client.batch.add_data_object(object_data, 'Books')
        except BaseException as error:
            print("Import Failed at: ", i)
            print("An exception occurred: {}".format(error))
            break
    client.batch.flush()

def create_games_schema(client):
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
            {"name": "CombinedVector", "dataType": ["number[]"]}
        ]
    }
    client.schema.create_class(tfidf_class_schema_games)

def process_games_data():
    df_games = pd.read_csv('games.csv', nrows=int(pd.read_csv('games.csv').shape[0]*0.25))
    df_games['Summary'] = df_games['Summary'].fillna('')
    df_games['Title'] = df_games['Title'].fillna('')
    df_games['Genres'] = df_games['Genres'].fillna('')
    df_games['Release_Date'] = df_games['Release_Date'].fillna(0)
    corpus_games = df_games['Title'] + " " + df_games['Summary'] + " " + df_games['Genres']
    vectorizer = TfidfVectorizer()
    X_games_combined = vectorizer.fit_transform(corpus_games)
    return X_games_combined, df_games

def add_objects_games(client, X_combined, df):
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
            'CombinedVector': X_combined[i].toarray().tolist()[0]
        }
        try:
            client.batch.add_data_object(object_data, 'Games')
        except BaseException as error:
            print("Import Failed at: ", i)
            print("An exception occurred: {}".format(error))
            break
    client.batch.flush()

def create_movies_schema(client):
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
            {"name": "CombinedVector", "dataType": ["number[]"]}
        ]
    }
    client.schema.create_class(tfidf_class_schema_movies)

def process_movies_data():
    df_movies = pd.read_csv('films2.csv', nrows=int(pd.read_csv('films2.csv').shape[0]*0.25))
    df_movies['Description'] = df_movies['Description'].fillna('')
    df_movies['Title'] = df_movies['Title'].fillna('')
    df_movies['Plot'] = df_movies['Plot'].fillna('')
    corpus_movies = df_movies['Title'] + " " + df_movies['Description'] + " " + df_movies['Plot']
    vectorizer = TfidfVectorizer()
    X_movies_combined = vectorizer.fit_transform(corpus_movies)
    return X_movies_combined, df_movies

def add_objects_movies(client, X_combined, df):
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
            client.batch.add_data_object(object_data, 'Movies')
        except BaseException as error:
            print("Import Failed at: ", i)
            print("An exception occurred: {}".format(error))
            break
    client.batch.flush()

def main():
    client = initialize_client()

    create_books_schema(client)
    X_books_combined, df_books = process_books_data()
    add_objects_books(client, X_books_combined, df_books)

    create_games_schema(client)
    X_games_combined, df_games = process_games_data()
    add_objects_games(client, X_games_combined, df_games)

    create_movies_schema(client)
    X_movies_combined, df_movies = process_movies_data()
    add_objects_movies(client, X_movies_combined, df_movies)

if __name__ == "__main__":
    main()
