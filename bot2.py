import telebot
from telebot import types
import pymysql
from bd import host, user, password, db_name
import mysql_queries
import mo
import joblib
from langdetect import detect, LangDetectException

bot = telebot.TeleBot('7187399307:AAG723Gw3Kglm5i6kEIw5xwQ0Q0nGzsnUA4')

def is_english(text):
    try:
        return detect(text) == 'en'
    except LangDetectException:
        return False


try:
    connection = pymysql.connect(
        host=host,
        port=3306,
        user=user,
        password=password,
        database=db_name,
        cursorclass=pymysql.cursors.DictCursor
    )
    print('connection is established')
except Exception as ex:
    print(ex)

book_vectorizer = joblib.load('tfidf_vectorizer_books.pkl')
game_vectorizer = joblib.load('tfidf_vectorizer_games.pkl')
movie_vectorizer = joblib.load('tfidf_vectorizer.pkl')


@bot.message_handler(commands=['start'])
def start(message):
    murkup_content_buttons = types.InlineKeyboardMarkup(row_width=1)
    movies_button = types.InlineKeyboardButton('Movies', callback_data='movies')
    games_button = types.InlineKeyboardButton('Games', callback_data='games')
    books_button = types.InlineKeyboardButton('Books', callback_data='books')
    murkup_content_buttons.add(movies_button, games_button, books_button)
    bot.send_message(message.chat.id, 'Hi! Choose the type of content you want to get recommendations for', reply_markup=murkup_content_buttons)


@bot.callback_query_handler(func=lambda call: call.data.startswith('movie_'))
def handle_movie_callback(call):
    callback_data = call.data.split('_')
    user_id = int(callback_data[1])
    title = callback_data[2]
    link = callback_data[3]
    id_query = mysql_queries.get_last_query(connection, user_id)
    mysql_queries.insert_user_replies_chosen(connection, id_query, str(title), 'movie')
    bot.send_message(call.message.chat.id, f"You've chosen: {title} \n link: {link}")


@bot.callback_query_handler(func=lambda call: call.data.startswith('book_'))
def handle_book_callback(call):
    callback_data = call.data.split('_')
    user_id = int(callback_data[1])
    title = callback_data[2]
    link = callback_data[3]
    id_query = mysql_queries.get_last_query(connection, user_id)
    mysql_queries.insert_user_replies_chosen(connection, id_query, str(title), 'book')
    bot.send_message(call.message.chat.id, f"You've chosen: {title} \n link: {link}")


@bot.callback_query_handler(func=lambda call: call.data.startswith('game_'))
def handle_game_callback(call):
    callback_data = call.data.split('_')
    user_id = int(callback_data[1])
    title = callback_data[2]
    link = callback_data[3]
    id_query = mysql_queries.get_last_query(connection, user_id)
    mysql_queries.insert_user_replies_chosen(connection, id_query, str(title), 'game')
    bot.send_message(call.message.chat.id, f"You've chosen: {title} \n link: {link}")


@bot.callback_query_handler(func=lambda call: True)
def callback(call):
    if call.message:
        if call.data == 'movies':
            bot.send_message(call.message.chat.id, 'Send your age')
            bot.register_next_step_handler(call.message, get_age_movies)
        elif call.data == 'books':
            bot.send_message(call.message.chat.id, 'Send your age')
            bot.register_next_step_handler(call.message, get_age_books)
        elif call.data == 'games':
            bot.send_message(call.message.chat.id, 'Send your age')
            bot.register_next_step_handler(call.message, get_age_games)


def get_age_movies(message):
    age = message.text
    if age.isdigit():
        bot.send_message(message.chat.id, 'Great! Now send me the plot')
        bot.register_next_step_handler(message, lambda msg: process_query_movies(msg, age))
    else:
        bot.send_message(message.chat.id, 'Please enter a valid number for your age')
        bot.register_next_step_handler(message, get_age_movies)


def get_age_books(message):
    age = message.text
    if age.isdigit():
        bot.send_message(message.chat.id, 'Great! Now send me the plot')
        bot.register_next_step_handler(message, lambda msg: process_query_books(msg, age))
    else:
        bot.send_message(message.chat.id, 'Please enter a valid number for your age')
        bot.register_next_step_handler(message, get_age_books)


def get_age_games(message):
    age = message.text
    if age.isdigit():
        bot.send_message(message.chat.id, 'Great! Now send me the plot')
        bot.register_next_step_handler(message, lambda msg: process_query_games(msg, age))
    else:
        bot.send_message(message.chat.id, 'Please enter a valid number for your age')
        bot.register_next_step_handler(message, get_age_games)

def process_query_books(message, age):
    if not is_english(message.text):
        bot.send_message(message.chat.id, 'Please provide the plot in English.')
        bot.register_next_step_handler(message, lambda msg: process_query_books(msg, age))
        return
    bot.send_message(message.chat.id, 'It is processing...')
    preprocessed_query = message.text.replace("'", "")

    age = int(age)
    user_query = message.text
    recommendations = mo.recommend_books(user_query, book_vectorizer, age)
    markup = types.InlineKeyboardMarkup(row_width=1)
    recommendations_string = ""
    for rec in recommendations:
        callback_data = f"book_{message.chat.id}_{rec['Title'][:10]}_{rec['link']}"
        button = types.InlineKeyboardButton(f"{rec['Title']} (Age: {rec['constraint']}+)", callback_data=callback_data)
        markup.add(button)
        recommendations_string += rec['Title'] + "\\"

    content_type = 'book'
    mysql_queries.insert(connection, preprocessed_query, message.chat.id, recommendations_string, content_type)

    text = 'Here are your recommendations:\n'

    for rec in recommendations:
        if int(rec['date'])==0:
            text += f"Name: {rec['Title']}\nYear: unknown\nAuthor: {rec['Author']}\nSummary: {rec['Summary'][:400]}\n\n"
        else:
            text += f"Name: {rec['Title']}\nYear: {rec['date']}\nAuthor: {rec['Author']}\nSummary: {rec['Summary'][:400]}...\n\n"
    bot.send_message(message.chat.id, text, reply_markup=markup)


def process_query_games(message, age):
    if not is_english(message.text):
        bot.send_message(message.chat.id, 'Please provide the plot in English.')
        bot.register_next_step_handler(message, lambda msg: process_query_games(msg, age))
        return
    bot.send_message(message.chat.id, 'It is processing...')
    preprocessed_query = message.text.replace("'", "")

    age = int(age)
    user_query = message.text
    recommendations = mo.recommend_games(user_query, game_vectorizer, age)
    markup = types.InlineKeyboardMarkup(row_width=1)
    recommendations_string = ""
    for rec in recommendations:
        callback_data = f"game_{message.chat.id}_{rec['Title'][:10]}_{rec['link']}"
        button = types.InlineKeyboardButton(f"{rec['Title']} (Age: {rec['constraint']}+)", callback_data=callback_data)
        markup.add(button)
        recommendations_string += rec['Title'] + "\\"

    content_type = 'game'
    mysql_queries.insert(connection, preprocessed_query, message.chat.id, recommendations_string, content_type)

    text = 'Here are your recommendations:\n'
    for rec in recommendations:
        text += f"Name: {rec['Title']}\nDate: {rec['date']}\nSummary: {rec['Summary'][:450]}\n\n"
    bot.send_message(message.chat.id, text, reply_markup=markup)

def process_query_movies(message,age):
    if not is_english(message.text):
        bot.send_message(message.chat.id, 'Please provide the plot in English.')
        bot.register_next_step_handler(message, lambda msg: process_query_movies(msg, age))
        return
    bot.send_message(message.chat.id, 'It is processing...')
    preprocessed_query= (message.text).replace("'", "")
    
    age = int(age)
    user_query = message.text
    recommendations = mo.recommend_movies(user_query, movie_vectorizer, age)
    markup = types.InlineKeyboardMarkup(row_width=1)
    recommendations_string=""
    for index, rec in enumerate(recommendations):
        callback_data = f"movie_{message.chat.id}_{rec['Title'][:10]}_{rec['link']}"
        button = types.InlineKeyboardButton(f"{rec['Title']} (Age: {rec['Constraint']}+)",callback_data=callback_data)
        markup.add(button)
        recommendations_string+=rec['Title']+"\\"

    content_type='movies'
    mysql_queries.insert(connection,preprocessed_query,message.chat.id,recommendations_string,content_type)
   
    text ='Here are your recommendations:\n'
    for index, rec in enumerate(recommendations):
        text+="Name: "+rec['Title']+"\n Description: "+(rec['Description'][:450])+"\n"+"year: "+str(rec['year'])+ "\ngenre: "+rec['genre']+"\n\n"
    bot.send_message(message.chat.id, text, reply_markup=markup)



bot.polling()

