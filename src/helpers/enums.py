"""Enumerations for supported SQL servers, connection targets, and authentication types."""
from enum import Enum


class SupportedSQLServers(Enum):
    """
    This class holds the supported SQL types by this project
    """
    MSSQL = 1
    PSQL = 2


class ServerToConnect(Enum):
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
