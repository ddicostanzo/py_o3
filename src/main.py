from api.o3_api import O3DataModel


if __name__ == "__main__":

    test = O3DataModel("./Resources/O3_20250128.json")
    test_sql = test.key_elements['Patient'].create_sql_table_text('MSSQL')
    print(test.key_elements)

