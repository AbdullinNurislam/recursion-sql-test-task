from parser import parse
from sql.create_db import create_db
from sql.scripts import create_table, fill_table, get_child_of_parents


def parse_json(filename='data.json'):
    """Получаем данные из файла формата json"""
    import json
    with open(filename) as f:
        data = json.load(f)
    return data


def prepare_table():
    """
    Создаём таблицу, если её нет в бд, и заполняем её данными
    """
    create_table()
    data = parse_json()
    fill_table(data=data)


def build(args):
    """
    Создаём БД, если её нет, и подготавливаем таблицу company_structure для дальнейшей работы с ней
    """
    create_db()
    prepare_table()


def get_employees(args):
    if args.employee_id:
        employees = get_child_of_parents(args.employee_id)
        print(f'Список сотрудников:\n{", ".join(employees)}')
    else:
        print('Please enter value for employee_id')


if __name__ == '__main__':
    actions = {
        'build': build,
        'employees': get_employees
    }
    args = parse()
    if actions.get(args.action, None):
        actions[args.action](args)
    else:
        print('Incorrect value of argument "action"')
