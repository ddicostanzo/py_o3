"""MSSQL database connection management using pyodbc and .env configuration."""
import warnings

import pyodbc
from dotenv import dotenv_values

from helpers.enums import ServerToConnect, SQLAuthentication

_REQUIRED_KEYS = ['DRIVER', 'SERVER', 'DATABASE', 'SCHEMA', 'AUTH', 'USERID', 'PASSWORD']


class MSSQLConnection:
    """
    Class holds the MS SQL connections for the O3 and Aura databases.
    """

    @classmethod
    def create_connection(cls, sql_server: ServerToConnect):
        """
        This method instantiates the database object using either the O3 or Aura
        ServerToConnect enum.

        Parameters
        ----------
        sql_server: ServerToConnect
            The type of server to connect.

        Returns
        ----------
        MSSQLConnection
            returns the MSSQLConnection object that is instantiated by the
            server and .env file.
        """
        if sql_server == ServerToConnect.O3:
            # Strip the "O3_" prefix from .env keys (e.g., "O3_SERVER" -> "SERVER")
            # so the config dict uses generic key names for the constructor
            return cls({k.split('_')[1]: v for k, v in dotenv_values().items() if 'O3' in k})
        elif sql_server == ServerToConnect.Aura:
            # Strip the "AURA_" prefix from .env keys (e.g., "AURA_DATABASE" -> "DATABASE")
            return cls({k.split('_')[1]: v for k, v in dotenv_values().items() if 'AURA' in k})
        else:
            raise ValueError("Provided server not supported.")

    def __init__(self, config):
        try:
            self.__driver = config['DRIVER']
            self.host = config['SERVER']
            self.database = config['DATABASE']
            self.schema = config['SCHEMA']
            self.authentication = SQLAuthentication(config['AUTH'].upper())
            self.__user = config['USERID']
            self.__password = config['PASSWORD']
        except KeyError as e:
            raise ValueError(
                f"Missing required config key {e}. "
                f"Required keys are: {', '.join(_REQUIRED_KEYS)}. "
                f"Keys must be prefixed with O3_* or AURA_* in the .env file "
                f"(e.g., O3_DRIVER, AURA_SERVER)."
            ) from e

        self.encrypt = config.get('ENCRYPT', 'yes')
        self.trust_server_cert = config.get('TRUSTSERVERCERTIFICATE', 'no')

        if self.encrypt != 'yes':
            warnings.warn(
                "Encrypt is not set to 'yes'. Connection may not be secure.",
                UserWarning,
                stacklevel=2,
            )
        if self.trust_server_cert == 'yes':
            warnings.warn(
                "TrustServerCertificate is set to 'yes'. "
                "Server certificate will not be validated.",
                UserWarning,
                stacklevel=2,
            )

    def __repr__(self) -> str:
        return (f"MSSQLConnection(host={self.host!r}, database={self.database!r}, "
                f"schema={self.schema!r}, auth={self.authentication.value!r})")

    def __connection_string(self) -> dict[str, str]:
        if self.authentication == SQLAuthentication.SQL:
            return {
                "Driver": self.__driver,
                "Server": self.host,
                "Database": self.database,
                "Trusted_Connection": 'no',
                "UID": self.__user,
                "PWD": self.__password,
                "Encrypt": self.encrypt,
                "TrustServerCertificate": self.trust_server_cert
            }
        elif self.authentication == SQLAuthentication.Integrated:
            return {
                "Driver": self.__driver,
                "Server": self.host,
                "Database": self.database,
                "Trusted_Connection": 'yes',
                "Encrypt": self.encrypt,
                "TrustServerCertificate": self.trust_server_cert
            }
        else:
            raise ValueError("Provided server not supported.")

    def connection(self) -> pyodbc.Connection:
        """
        Provides access to the pyodbc.Connection object that is generated using the
        connection string of the object. That connection string is generated from the .env
        file and which server is being connected to.
        """
        return pyodbc.connect(';'.join([f'{k}={v}' for k, v in self.__connection_string().items()]))


if __name__ == "__main__":
    pass
