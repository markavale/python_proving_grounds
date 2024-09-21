from database.mongo_helper import MongoDbConnection


def main():
    mongo_instance = MongoDbConnection()
    mongo_instance_2 = MongoDbConnection()

    print(mongo_instance)
    print(mongo_instance_2)

    print(mongo_instance == mongo_instance_2)

if __name__ == "__main__":
    main()
