'''
    Parser and validity checker
'''

from type_def import Types


def match_token(token: str) -> Types | str:
    match token:
        case 'exit' | 'stop' | 'quit':
            return Types.EXIT
        case 'create':
            return Types.CREATE
        case 'drop':
            return Types.DROP
        case 'database':
            return Types.DATABASE
        case 'table':
            return Types.TABLE
        case 'index':
            return Types.INDEX
        case 'use':
            return Types.USE
        case 'insert':
            return Types.INSERT
        case 'into':
            return Types.INTO
        case 'values':
            return Types.VALUES
        case 'where':
            return Types.WHERE
        case 'delete':
            return Types.DELETE
        case 'from':
            return Types.FROM
        case 'add':
            return Types.ADD
        case 'pk':
            return Types.PK
        case 'fk':
            return Types.FK
        case 'uq':
            return Types.UQ
        case '*':
            return Types.ALL
        case 'select':
            return Types.SELECT

        # operators
        case '=' | '==':
            return Types.EQ
        case '<':
            return Types.LT
        case '>':
            return Types.GT
        case '<=':
            return Types.LE
        case '>=':
            return Types.GE
        case '!=' | '<>':
            return Types.NE

        # data types
        case 'int':
            return Types.INT
        case 'float':
            return Types.FLOAT
        case 'bit':
            return Types.BIT
        case 'date':
            return Types.DATE
        case 'datetime':
            return Types.DATETIME
        case 'string' | 'varchar':
            return Types.STRING
        case _:
            return token


def tokenize(commands: str) -> list:
    command_list = commands.split(' ')

    if len(command_list) == 0:
        return []

    stripped = map(lambda token: token.lower().strip(', ()\t\n\r\'"'),
                   command_list)
    tokenized = map(lambda token: match_token(token), stripped)

    return list(tokenized)


def parse(list_of_commands: list) -> bool:
    '''
        Checks the correctness of the commands
    '''

    match list_of_commands:
        case [Types.EXIT]:
            return True
        case [Types.USE, db]:
            return isinstance(db, str)
        case [Types.CREATE, Types.DATABASE, db]:
            return isinstance(db, str)
        case [Types.CREATE, Types.TABLE, table, *args]:
            return isinstance(table, str) and check_create_table_args(args)
        case [Types.DROP, Types.DATABASE, db]:
            return isinstance(db, str)
        case [Types.DROP, Types.TABLE, table]:
            return isinstance(table, str)
        case [Types.ADD, Types.INDEX, table, col]:
            return isinstance(table, str) and isinstance(col, str)
        case [Types.INSERT, Types.INTO, table, Types.VALUES, *args]:
            return isinstance(table, str) and len(args) != 0
        case [Types.DELETE, Types.FROM, table, Types.WHERE, id]:
            return isinstance(table, str) and isinstance(id, str)
        case [Types.ADD, Types.PK, table, col]:
            return isinstance(table, str) and isinstance(col, str)
        case [Types.ADD, Types.FK, table1, col1, table2, col2]:
            return (isinstance(table1, str) and isinstance(col1, str)
                    and isinstance(table2, str) and isinstance(col2, str))
        case [Types.ADD, Types.UQ, table, col]:
            return isinstance(table, str) and isinstance(col, str)
        case [Types.SELECT, Types.ALL, Types.FROM, table] if\
                isinstance(table, str):
            return True
        case [Types.SELECT, Types.ALL, Types.FROM, *args]:
            return check_select_all_args(args)
        case [Types.SELECT, *args]:
            return check_select_args(args)
        case _:
            return False


def check_select_all_args(args: list) -> bool:
    prev = True
    for idx, val in enumerate(args):
        if idx % 2:
            if prev and val not in (Types.EQ, Types.LE, Types.GT,
                                    Types.LE, Types.GE, Types.NE):
                return False

            if not prev and val not in (Types.AND, Types.OR):
                return False

            prev = not prev

    return True


def check_select_args(args: list) -> bool:
    return True
        
    if len(args) < 7:
        return False

    # check FROM
    from_idx = -1
    for idx, val in enumerate(args):
        if val == Types.FROM and idx != 0:
            from_idx = idx
            break

    if from_idx == -1 or from_idx == len(args) - 1:
        return False

    # check where
    where_idx = -1
    for idx in range(from_idx + 1, len(args)):
        val = args[idx]
        if val == Types.WHERE:
            where_idx = idx
            break

    if where_idx == -1 or where_idx == len(args) - 1:
        return False

    prev = True
    for idx in range(where_idx + 1, len(args)):
        val = args[idx]

        if idx % 2:
            if prev and val not in (Types.EQ, Types.LE, Types.GT,
                                    Types.LE, Types.GE, Types.NE):
                return False

            if not prev and val not in (Types.AND, Types.OR):
                return False

            prev = not prev

    return True


def check_create_table_args(args: list) -> bool:
    if len(args) % 2 == 1:
        return False

    names = set()

    cursor = 0
    while cursor < len(args):
        if (not isinstance(args[cursor], Types) or
                not isinstance(args[cursor + 1], str)):
            return False

        if args[cursor + 1] in names:
            return False
        names.add(args[cursor + 1])

        cursor += 2

    return True
