import psycopg2
from psycopg2 import Error
from settings import USER_MODE_PARAMS


class DBConnector:
    """
    Класс для работы с таблицей company_structure
    """

    def __init__(self, db_params=None):
        db_params = db_params or USER_MODE_PARAMS
        self.connection = psycopg2.connect(**db_params)

    def action(self, cursor):
        pass

    def execute(self, query_string, *args):
        with self.connection.cursor() as cursor:
            try:
                cursor.execute(query_string, *args)
                return self.action(cursor)
            except (Exception, Error) as error:
                print("Ошибка при работе с PostgreSQL", error)


class FetchOneConnector(DBConnector):

    def action(self, cursor):
        return cursor.fetch_one()


class FetchAllConnector(DBConnector):

    def action(self, cursor):
        return cursor.fetch_all()


class CommitConnector(DBConnector):

    def action(self, cursor):
        self.connection.commit()
