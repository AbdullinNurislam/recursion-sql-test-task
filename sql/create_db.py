from settings import ADMIN_MODE_PARAMS, USER_MODE_PARAMS
from db_connector import FetchOneConnector, CommitConnector


def create_db():
    """От имени пользователя postgres создаём базу, если её нет"""
    db_exist_query = f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{USER_MODE_PARAMS['database']}'"
    exists = FetchOneConnector(ADMIN_MODE_PARAMS).execute(db_exist_query)

    if not exists:
        print(f'БД "{USER_MODE_PARAMS["database"]}" не существует, создадим  её')
        CommitConnector(ADMIN_MODE_PARAMS).execute(f'CREATE DATABASE {USER_MODE_PARAMS["database"]}')
        CommitConnector(ADMIN_MODE_PARAMS).execute(
            f'GRANT ALL PRIVILEGES ON DATABASE {USER_MODE_PARAMS["database"]} TO "{USER_MODE_PARAMS["user"]}";'
        )
    else:
        print(f'БД "{USER_MODE_PARAMS["database"]}" уже существует')
