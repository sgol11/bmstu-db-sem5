from db_work import DatabaseQueryExecutor
import configparser


def menu() -> None:
    print("\n_____________Меню_____________\n")
    print("Выбрать действие:")
    print("1. Выполнить скалярный запрос")
    print("2. Выполнить запрос с несколькими соединениями (JOIN)")
    print("3. Выполнить запрос с ОТВ(CTE) и оконными функциями")
    print("4. Выполнить запрос к метаданным")
    print("5. Вызвать скалярную функцию")
    print("6. Вызвать подставляемую табличную функцию")
    print("7. Вызвать хранимую процедуру")
    print("8. Вызвать системную функцию")
    print("9. Создать таблицу в базе данных, соответствующую тематике БД")
    print("10. Выполнить вставку данных в созданную таблицу с использованием инструкции INSERT")
    print("0. Выход\n")


def menu_loop() -> None:
    config = configparser.ConfigParser()
    config.read("cfg.ini")
    db_query_ex = DatabaseQueryExecutor(config["crimes_db"]["host"],
                                        config["crimes_db"]["user"],
                                        config["crimes_db"]["password"],
                                        config["crimes_db"]["db_name"],
                                        config["crimes_db"]["port"])

    while True:
        menu()
        action = input("Введите действие: ")

        if action == "0":
            break
        elif action == "1":
            response = db_query_ex.scalarQuery()
            print("Имя подозреваемого: ", end="")
        elif action == "2":
            response = db_query_ex.joinQuery()
            print("Подозреваемые: ")
        elif action == "3":
            response = db_query_ex.CTEWindowFunctionQuery()
            print("Решенные дела отдела: ")
        elif action == "4":
            response = db_query_ex.metaQuery()
            print("Информация о столбцах таблицы: ")
        elif action == "5":
            response = db_query_ex.scalarFuncQuery()
            print("Максимальное число раскрытых преступлений: ", end="")
        elif action == "6":
            response = db_query_ex.tabularFuncQuery()
            print("Преступления подозреваемого: ")
        elif action == "7":
            response = db_query_ex.storedProcCall()
            print("После перевода: ")
        elif action == "8":
            response = db_query_ex.systemFuncQuery()
        elif action == "9":
            response = db_query_ex.createNewTable()
        elif action == "10":
            response = db_query_ex.insertIntoNewTable()
        else:
            print("Неправильный пункт меню")
            continue

        for row in response:
            print(row)
