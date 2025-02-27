from src.helpers.enums import SupportedSQLServers

sql_data_types = {SupportedSQLServers.MSSQL: {"Boolean": "bit",
                                              "Binary": "varbinary",
                                              "Date": "datetime2",
                                              "Decimal": "decimal(19,9)",
                                              "Integer": "int",
                                              "String": "varchar(max)"
                                              },
                  SupportedSQLServers.PSQL: {"Boolean": "boolean",
                                             "Binary": "bytea",
                                             "Date": "timestamptz",
                                             "Decimal": "numeric(19,9)",
                                             "Integer": "integer",
                                             "String": "text"
                                             }
                  }

if __name__ == "__main__":
    pass
