import pymysql
from bd import host, user, password,db_name



def get_last_query(connection, id):
     with connection.cursor() as cursor:
            cursor.execute(f"SELECT id FROM users.user_queries WHERE id_user = %s ORDER BY id DESC LIMIT 1;", (id,))
            result = cursor.fetchone()
            if result:
                return result['id']
            else:
                return None
    

# try:
#     connection=pymysql.connect(
#         host=host,
#         port=3306,
#         user=user,
#         password=password,
#         database=db_name,
#         cursorclass=pymysql.cursors.DictCursor
#     )
    
#     # try:
#     #     with connection.cursor() as cursor:
#     #         cursor.execute("SELECT * FROM `user_queries`")
#     #         rows = cursor.fetchall()
#     #         for row in rows:
#     #             print(row)
#     #         # try:
#     #         #     cursor.execute("INSERT INTO users.user_queries (id_user, query) VALUES (12345678, 'shes just a girl')")
#     #         #     connection.commit()
#     #         # except Exception as exc:
#     #         #     print(exc)
#     # except:
#     #     connection.close()
#     try:
#         with connection.cursor() as cursor:
#             cursor.execute("SELECT * FROM `user_recommendations`")
#             rows = cursor.fetchall()
#             for row in rows:
#                 print(row)
#             # cursor.execute("ALTER TABLE users.user_recommendations DROP COLUMN content_type")
#             # cursor.execute("ALTER TABLE users.user_queries ADD COLUMN content_type VARCHAR(20)")
#             # connection.commit()
#     except Exception as ex:
#         print(ex)
# except Exception as ex:
#     print(ex)

def insert(connection, query, id, recomendations,content_type):
    try:
        try:
            with connection.cursor() as cursor:
                cursor.execute("INSERT INTO users.user_queries (id_user, query, recommendations, content_type) VALUES (%s, %s, %s, %s)", (id, query, recomendations, content_type))
                connection.commit()
        except Exception as exc:
                print(exc)
    except:
        connection.close()



def insert_user_replies_chosen(connection,id_query, chosen,type):
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"INSERT INTO users.user_recommendations (id_query,content_type, chosen_content) VALUES ({id_query},'{type}','{chosen}')")
                connection.commit()
        except Exception as exc:
                print(exc)
    
    

# CREATE TABLE users.user_queries(
#     id INT PRIMARY KEY AUTO_INCREMENT,
#     id_user INT NOT NULL,
#     query VARCHAR(450) NOT NULL,
#     recommendations TEXT,
#     content_type VARCHAR(20)
# );

# CREATE TABLE users.user_recommendations (
#     id INT PRIMARY KEY AUTO_INCREMENT,
#     id_query INT NOT NULL,
#     content_type VARCHAR(50) NOT NULL,
#     chosen_content VARCHAR(100) NOT NULL,
#     FOREIGN KEY (id_query) REFERENCES user_queries(id)
# );