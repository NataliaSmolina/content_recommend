import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
import joblib


# df_books = pd.read_csv('books.csv', nrows=int(pd.read_csv('books.csv').shape[0]*0.25))
df_books = pd.read_csv('books.csv')

df_books['summary'] = df_books['summary'].fillna('')
df_books['book_title'] = df_books['book_title'].fillna('')
df_books['author'] = df_books['author'].fillna('')


corpus_books = df_books['book_title'] + " " + df_books['summary'] + " " + df_books['author']

vectorizer = TfidfVectorizer()
X_books_combined = vectorizer.fit_transform(corpus_books)
joblib.dump(vectorizer, 'tfidf_vectorizer_books.pkl')