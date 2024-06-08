import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
import pandas as pd
import joblib



# Загрузка данных (первые 25%)
# df_games = pd.read_csv('games.csv', nrows=int(pd.read_csv('games.csv').shape[0]*0.25))
df_games = pd.read_csv('games.csv')
# Обработка данных
df_games['Summary'] = df_games['Summary'].fillna('')
df_games['Title'] = df_games['Title'].fillna('')
df_games['Genres'] = df_games['Genres'].fillna('')

# Создание корпуса текстов
corpus_games = df_games['Title'] + " " + df_games['Summary'] + " " + df_games['Genres']

# Преобразование векторов TF-IDF и сохранение векторизатора
vectorizer = TfidfVectorizer()
X_games_combined = vectorizer.fit_transform(corpus_games)
joblib.dump(vectorizer, 'tfidf_vectorizer_games.pkl')


