from settings import EMPLOYEE_TABLE_NAME
from db_connector import FetchAllConnector, FetchOneConnector, CommitConnector


def create_table():
    """Создаём в БД таблицу company_structure с полями Id, ParentId, Name и Type"""
    create_table_query = f'''CREATE TABLE IF NOT EXISTS {EMPLOYEE_TABLE_NAME}
                      (Id       INT     PRIMARY KEY     NOT NULL,
                      ParentId  INT,
                      Name      TEXT    NOT NULL,
                      Type      INT     NOT NULL);'''
    CommitConnector().execute(create_table_query)


def fill_table(data):
    """
    Заполняем таблицу company_structure данными, которые получили из файла data.json
    """

    fill_table_query = f'INSERT INTO {EMPLOYEE_TABLE_NAME} (Id, ParentId, Name, Type) values (%s, %s, %s, %s);'
    table_is_not_empty_query = f'SELECT count(*) FROM {EMPLOYEE_TABLE_NAME}'
    rows_count = FetchOneConnector().execute(table_is_not_empty_query)

    if rows_count > 0:
        print(f'В таблице "{EMPLOYEE_TABLE_NAME}" есть данные, пропускаем заполнение')
        return
    for row in data:
        CommitConnector().execute(fill_table_query, (row['id'], row['ParentId'], row['Name'], row['Type']))
    print(f'Таблица "{EMPLOYEE_TABLE_NAME}" заполнена данными')


def get_child_of_parents(unit_id=None, child_type_id=3, parent_type_id=1):
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
        {EMPLOYEE_TABLE_NAME}
      WHERE
        Id = {unit_id}
      UNION
        SELECT
          es2.Id,
          es2.ParentId,
          es2.Name,
          es2.Type
        FROM
          {EMPLOYEE_TABLE_NAME} es2
        INNER JOIN parents s ON s.ParentId = es2.Id
    ) SELECT * FROM parents WHERE Type = {parent_type_id}
        UNION
            SELECT
                es2.Id,
                es2.ParentId,
                es2.Name,
                es2.Type
            FROM
                {EMPLOYEE_TABLE_NAME} es2
            INNER JOIN child s ON s.Id = es2.ParentId
    ) SELECT
        Name
    FROM
        child WHERE Type={child_type_id};
    '''

    result = FetchAllConnector().execute(get_employees)
    result = [empolyee_name[0] for empolyee_name in result]
    return result
