import os
from dotenv import dotenv_values
from helpers.enums import SQLServerConnect, SQLAuthentication
import pyodbc


class MSSQLConnection:

    @classmethod
    def create_connection(cls, sql_server: SQLServerConnect):

        if sql_server == SQLServerConnect.O3:
            return cls({k.split('_')[1]: v for k, v in dotenv_values().items() if 'O3' in k})
        elif sql_server == SQLServerConnect.Aura:
            return cls({k.split('_')[1]: v for k, v in dotenv_values().items() if 'AURA' in k})
        else:
            raise ValueError("Provided server not supported.")

    def __init__(self, config):
        self.__driver = config['DRIVER']
        self.host = config['SERVER']
        self.database = config['DATABASE']
        self.schema = config['SCHEMA']
        self.authentication = SQLAuthentication(config['AUTH'].upper())
        self.__user = config['USERID']
        self.__password = config['PASSWORD']
        self.trust_server_cert = config['TRUSTSERVERCERTIFICATE']
        self.encrypt = config['ENCRYPT']

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
        connection_string = ';'.join([f'{k}={v}' for k, v in self.__connection_string().items()])
        return pyodbc.connect(';'.join([f'{k}={v}' for k, v in self.__connection_string().items()]))


if __name__ == "__main__":
    pass
