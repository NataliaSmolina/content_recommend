import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from weaviate import Client
from sklearn.metrics.pairwise import cosine_similarity
import pandas as pd
import joblib


client = Client("http://localhost:8080")

def get_book_vectors(client,age):
    response = client.query.get("Books", ["combinedVector", "book_title", "summary", "author","constraint","link","publication_date"]).do()
    books = response['data']['Get']['Books']
    
    book_vectors = []
    book_titles = []
    book_summaries = []
    book_authors = []
    book_links = []
    book_constraints = []
    book_date = []
    
    for book in books:
        if int(book['constraint']) <= age:
            combined_vector = np.array(book['combinedVector'])
            book_vectors.append(combined_vector)
            book_titles.append(book['book_title'])
            book_summaries.append(book['summary'])
            book_authors.append(book['author'])
            book_links.append(book['link'])
            book_constraints.append(book['constraint'])
            book_date.append(book['publication_date'])
    return np.array(book_vectors), book_titles, book_summaries, book_authors,book_links,book_constraints,book_date

def recommend_books(user_query, vectorizer,age):
    book_vectors, book_titles, book_summaries, book_authors,book_links, book_constraints, book_date= get_book_vectors(client,age)
    user_query_vector = vectorize_query(user_query, vectorizer)
    cosine_similarities = cosine_similarity(user_query_vector, book_vectors)
    top_5_indices = np.argsort(cosine_similarities[0])[::-1][:5]

    recommendations = []
    for index in top_5_indices:
        recommendations.append({
            "Title": book_titles[index],
            "Summary": book_summaries[index],
            "Author": book_authors[index],
            "Cosine Similarity": cosine_similarities[0][index],
            "link":book_links[index],
            "date":book_date[index],
            "constraint":book_constraints[index]
        })
    return recommendations


def get_game_vectors(client,age):
    response = client.query.get("Games", ["combinedVector", "title", "summary", "release_Date","genres","constraint","link"]).do()
    games = response['data']['Get']['Games']
    
    game_vectors = []
    game_titles = []
    game_summaries = []
    game_genres = []
    game_constraints = []
    game_link = []
    game_dates=[]

    for game in games:
        if int(game['constraint']) <= age:
            combined_vector = np.array(game['combinedVector'])
            game_vectors.append(combined_vector)
            game_titles.append(game['title'])
            game_summaries.append(game['summary'])
            game_genres.append(game['genres'])
            game_constraints.append(game['constraint'])
            game_link.append(game['link'])
            game_dates.append(game['release_Date'])
    
    return np.array(game_vectors), game_titles, game_summaries, game_genres, game_constraints, game_link, game_dates

def recommend_games(user_query, vectorizer,age):
    game_vectors, game_titles, game_summaries, game_genres, game_constraints, game_link,game_dates = get_game_vectors(client,age)
    user_query_vector = vectorize_query(user_query, vectorizer)
    cosine_similarities = cosine_similarity(user_query_vector, game_vectors)
    top_5_indices = np.argsort(cosine_similarities[0])[::-1][:5]

    recommendations = []
    for index in top_5_indices:
        recommendations.append({
            "Title": game_titles[index],
            "Summary": game_summaries[index],
            "Genres": game_genres[index],
            "Cosine Similarity": cosine_similarities[0][index],
            "link":game_link[index],
            "constraint":game_constraints[index],
            "date":game_dates[index]
        })
    return recommendations

# Функции для фильмов
def get_movie_vectors(client, age):
    response = client.query.get("Movies", ["combinedVector", "title", "description","constraint","link","year","genre"]).do()
    movies = response['data']['Get']['Movies']
    
    movie_vectors = []
    movie_titles = []
    movie_descriptions = []
    movie_constraints = []
    movie_links = []
    movie_years=[]
    movie_genres=[]
    
    for movie in movies:
        if int(movie['constraint']) <= age:
            combined_vector = np.array(movie['combinedVector'])
            movie_vectors.append(combined_vector)
            movie_titles.append(movie['title'])
            movie_descriptions.append(movie['description'])
            movie_constraints.append(movie['constraint'])
            movie_links.append(movie['link'])
            movie_years.append(movie['year'])
            movie_genres.append(movie['genre'])

    
    return np.array(movie_vectors), movie_titles, movie_descriptions, movie_constraints, movie_links, movie_years, movie_genres

def recommend_movies(user_query, vectorizer,age):
    movie_vectors, movie_titles, movie_descriptions, movie_constraints, movie_links, movie_years, movie_genres = get_movie_vectors(client,age)
    user_query_vector = vectorize_query(user_query, vectorizer)
    cosine_similarities = cosine_similarity(user_query_vector, movie_vectors)
    top_5_indices = np.argsort(cosine_similarities[0])[::-1][:5]

    recommendations = []
    for index in top_5_indices:
        recommendations.append({
            "Title": movie_titles[index],
            "Description": movie_descriptions[index],
            "Constraint":movie_constraints[index],
            "Cosine Similarity": cosine_similarities[0][index],
            "link":movie_links[index],
            "year":movie_years[index],
            "genre":movie_genres[index]
        })
    return recommendations


def vectorize_query(query, vectorizer):
    
    query_vector = vectorizer.transform([query])
    return query_vector.toarray()

# def main():
#     # Загрузка моделей векторизации
#     book_vectorizer = joblib.load('tfidf_vectorizer_books.pkl')
#     game_vectorizer = joblib.load('tfidf_vectorizer_games.pkl')
#     movie_vectorizer = joblib.load('tfidf_vectorizer.pkl')

#     # Примеры запросов
#     book_query = "A science fiction book with a thrilling plot and complex characters"
#     game_query = "An action game with an epic storyline and thrilling battles"
#     movie_query = "A man falls in love with a girl and there are different problems with their love"

#     # Получение рекомендаций
#     book_recommendations = recommend_books(book_query, book_vectorizer, 18)
#     game_recommendations = recommend_games(game_query, game_vectorizer, 18)
#     movie_recommendations = recommend_movies(movie_query, movie_vectorizer,18)

#     # Вывод рекомендаций
#     print("Book Recommendations:")
#     for rec in book_recommendations:
#         print(f"Title: {rec['Title']}")
#         print(f"Summary: {rec['Summary']}")
#         print(f"Author: {rec['Author']}")
#         print(f"Cosine Similarity: {rec['Cosine Similarity']}")
#         print("-------------------------------")

#     print("Game Recommendations:")
#     for rec in game_recommendations:
#         print(f"Title: {rec['Title']}")
#         print(f"Summary: {rec['Summary']}")
#         print(f"Genres: {rec['Genres']}")
#         print(f"Cosine Similarity: {rec['Cosine Similarity']}")
#         print("-------------------------------")

#     print("Movie Recommendations:")
#     for rec in movie_recommendations:
#         print(f"Title: {rec['Title']}")
#         print(f"Description: {rec['Description']}")
#         print(f"Cosine Similarity: {rec['Cosine Similarity']}")
#         print("-------------------------------")

# if __name__ == "__main__":
#     main()
