import argparse


def parse():
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

    return args