from enum import Enum


class SupportedSQLServers(Enum):
    """
    This class holds the supported SQL types by this project
    """
    MSSQL = 1
    PSQL = 2


class SQLColumnDataTypes(Enum):
    """
    This class holds the supported data types for each column in O3
    """
    Boolean = 1
    Binary = 2
    Date = 3
    Decimal = 4
    Integer = 5
    String = 6


class SQLServerConnect(Enum):
    """
    Class holds the available servers to connect the software to
    """
    Aura = 'Aura'
    O3 = 'O3'


class SQLAuthentication(Enum):
    """
    Class holds the available authentication types for SQL servers
    """
    SQL = 'SQL'
    Integrated = 'INTEGRATED'


if __name__ == "__main__":
    pass
