'''
    Enum for the parser
'''

from enum import IntEnum, auto


class Types(IntEnum):

    # COMMANDS
    EXIT = auto()
    CREATE = auto()
    DROP = auto()
    DATABASE = auto()
    TABLE = auto()
    INDEX = auto()
    USE = auto()
    INSERT = auto()
    DELETE = auto()
    ADD = auto()
    PK = auto()
    FK = auto()
    UQ = auto()
    INTO = auto()
    VALUES = auto()
    FROM = auto()
    WHERE = auto()
    ALL = auto()
    SELECT = auto()
    COLUMNS = auto()

    # operators
    EQ = auto()
    LT = auto()
    GT = auto()
    LE = auto()
    GE = auto()
    NE = auto()
    AND = auto()
    OR = auto()

    # TYPES
    INT = auto()
    FLOAT = auto()
    BIT = auto()
    DATE = auto()
    DATETIME = auto()
    STRING = auto()
