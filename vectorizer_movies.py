import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
import joblib


# df_movies = pd.read_csv('films2.csv', nrows=int(pd.read_csv('films2.csv').shape[0]*0.25))
df_movies = pd.read_csv('films2.csv')

df_movies['Description'] = df_movies['Description'].fillna('')
df_movies['Title'] = df_movies['Title'].fillna('')
df_movies['Plot'] = df_movies['Plot'].fillna('')


corpus_movies = df_movies['Title'] + " " + df_movies['Description'] + " " + df_movies['Plot']


vectorizer = TfidfVectorizer()
vectorizer.fit(corpus_movies)

joblib.dump(vectorizer, 'tfidf_vectorizer.pkl')
