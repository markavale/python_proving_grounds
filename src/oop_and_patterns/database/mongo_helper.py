from design_pattern.singleton import SingletonMeta, singleton_dec


# class MongoDbConnection(metaclass=SingletonMeta):
@singleton_dec
class MongoDbConnection():
    def __init__(self) -> None:
        print("Mongo connection initialised")

class MySqlonnection():
    def __init__(self) -> None:
        print("Mongo connection initialised")