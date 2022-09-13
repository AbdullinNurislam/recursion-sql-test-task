from settings import DEFAULT_DB, POSTGRES_PASSWORD
import psycopg2
from psycopg2 import Error
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def create_db():
    """От имени пользователя postgres создаём базу, если её нет"""
    connection = None
    try:
        connection = psycopg2.connect(user='postgres',
                                      # пароль, который указали при установке PostgreSQL
                                      password=POSTGRES_PASSWORD,
                                      host=DEFAULT_DB['host'],
                                      port=DEFAULT_DB['port'])
        connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = connection.cursor()

        cursor.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{DEFAULT_DB['database']}'")
        exists = cursor.fetchone()
        if not exists:
            print(f'БД "{DEFAULT_DB["database"]}" не существует, создадим  её')
            cursor.execute(f'CREATE DATABASE {DEFAULT_DB["database"]}')
            cursor.execute(f'GRANT ALL PRIVILEGES ON DATABASE {DEFAULT_DB["database"]} TO "{DEFAULT_DB["user"]}";')
        else:
            print(f'БД "{DEFAULT_DB["database"]}" уже существует')

    except (Exception, Error) as error:
        print('Ошибка при работе с PostgreSQL', error)
    finally:
        if connection:
            cursor.close()
            connection.close()
            print('Соединение с PostgreSQL закрыто')
