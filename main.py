import argparse
from sql.create_db import create_db
from sql.scripts import CompanyStructure


def parse_json(filename='data.json'):
    """Получаем данные из файла формата json"""
    import json
    with open(filename) as f:
        data = json.load(f)
    return data


def prepare_table(company_structure):
    """
    Создаём таблицу, если её нет в бд, и заполняем её данными
    """
    company_structure.create_table()
    data = parse_json()
    company_structure.fill_table(data=data)


def build():
    """
    Создаём БД, если её нет, и подготавливаем таблицу company_structure для дальнейшей работы с ней
    """
    create_db()
    company_structure = CompanyStructure()
    prepare_table(company_structure)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Hierarchical retrieval",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        "action",
        help='''choose from next values: "build", "employees".
        "build" - creating database with table and fill by data from json.
        "employees" - get list of employees in office by employee_id.'''
    )
    parser.add_argument(
        '--employee_id',
        type=int,
        default=None,
        help='provide an integer (default: None)'
    )
    args = parser.parse_args()

    if args.action == 'build':
        build()
    elif args.action == 'employees':
        if args.employee_id:
            company_structure = CompanyStructure()
            employees = company_structure.get_child_of_parents(args.employee_id)
            print(f'Список сотрудников:\n{", ".join(employees)}')
        else:
            print('Please enter value for employee_id')
    else:
        print('Incorrect value of argument "action"')
