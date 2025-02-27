from api.data_model import O3DataModel


if __name__ == "__main__":

    test = O3DataModel("./Resources/O3_20250128.json", clean=True)
    print(test.key_elements)

