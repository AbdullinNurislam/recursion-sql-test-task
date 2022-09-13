import psycopg2
from psycopg2 import Error
from settings import DEFAULT_DB


class CompanyStructure:
    """
    Класс для работы с таблицей company_structure
    """

    def __init__(self):
        self.table_name = 'company_structure'
        self.connection = psycopg2.connect(**DEFAULT_DB)

    def create_table(self):
        """Создаём в БД таблицу company_structure с полями Id, ParentId, Name и Type"""
        create_table_query = f'''CREATE TABLE IF NOT EXISTS {self.table_name}
                          (Id       INT     PRIMARY KEY     NOT NULL,
                          ParentId  INT,
                          Name      TEXT    NOT NULL,
                          Type      INT     NOT NULL);'''
        with self.connection.cursor() as cursor:
            try:
                cursor.execute(create_table_query)
                self.connection.commit()
                print(f'Таблица "{self.table_name}" успешно создана в PostgreSQL')
            except (Exception, Error) as error:
                print("Ошибка при работе с PostgreSQL", error)

    def fill_table(self, data):
        """
        Заполняем таблицу company_structure данными, которые получили из файла data.json
        """

        fill_table_query = f'INSERT INTO {self.table_name} (Id, ParentId, Name, Type) values (%s, %s, %s, %s);'
        with self.connection.cursor() as cursor:
            try:
                cursor.execute(f'SELECT count(*) FROM {self.table_name}')
                rows_count = cursor.fetchone()[0]
                if rows_count > 0:
                    print(f'В таблице "{self.table_name}" есть данные, пропускаем заполнение')
                    return
                for row in data:
                    cursor.execute(fill_table_query, (row['id'], row['ParentId'], row['Name'], row['Type']))
                self.connection.commit()
                print(f'Таблица "{self.table_name}" заполнена данными')
            except (Exception, Error) as error:
                print("Ошибка при работе с PostgreSQL", error)

    def get_child_of_parents(self, unit_id=None, child_type_id=3, parent_type_id=1):
        """
        Получение всех записей типа child_type_id, являющихся дочерними для записи типа parent_type_id,
        для которых запись типа parent_type_id является родительской для записи с id равным значению unit_id
        Типы могут быть в следующих значениях:
            1 - офис в некотором городе
            2 - отдел офиса
            3 - сотрудник отдела

        По умолчанию получаем список всех сотрудников (Type=3) конкретного офиса (Type=1),
        в котором работает сотрудник с id=unit_id
        """
        if not unit_id:
            return []
        get_employees = f'''
        WITH RECURSIVE child AS (
            WITH
        RECURSIVE parents AS (
          SELECT
            Id,
            ParentId,
            Name,
            Type
          FROM
            {self.table_name}
          WHERE
            Id = {unit_id}
          UNION
            SELECT
              es2.Id,
              es2.ParentId,
              es2.Name,
              es2.Type
            FROM
              {self.table_name} es2
            INNER JOIN parents s ON s.ParentId = es2.Id
        ) SELECT * FROM parents WHERE Type = {parent_type_id}
            UNION
                SELECT
                    es2.Id,
                    es2.ParentId,
                    es2.Name,
                    es2.Type
                FROM
                    {self.table_name} es2
                INNER JOIN child s ON s.Id = es2.ParentId
        ) SELECT
            Name
        FROM
            child WHERE Type={child_type_id};
        '''

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(get_employees)
                result = cursor.fetchall()
                result = [empolyee_name[0] for empolyee_name in result]
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL", error)
        return result
