from api.o3_api import O3DataModel
import sys
import os

if __name__ == "__main__":
    #  test = O3DataModel("./Resources/O3_20250128.json")
    print(os.getcwd())

    test = O3DataModel(f"{os.getcwd()}/Resources/O3_20250128.json")
    print(test.key_elements)

