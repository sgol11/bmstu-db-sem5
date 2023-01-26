import psycopg2


class DatabaseQueryExecutor(object):
    def __init__(self, host, user, password, db_name, port):
        try:
            self.__connection = psycopg2.connect(
                host=host,
                user=user,
                password=password,
                database=db_name,
                port=port
            )
        except Exception as err:
            print("Error while connecting to database", err)
            return

        self.__connection.autocommit = True
        self.__cursor = self.__connection.cursor()

        print("PostgreSQL connection opened")

    def __del__(self):
        if self.__connection:
            self.__cursor.close()
            self.__connection.close()
            print("PostgreSQL connection closed")

    def scalarQuery(self) -> list:
        print("[Получить имя подозреваемого по его id]")

        suspect_id = int(input("Введите id подозреваемого (от 5000 до 6000): "))

        sql = """
                 select full_name
                 from suspects
                 where suspect_id = %s 
              """ % str(suspect_id)

        self.__cursor.execute(sql)

        db_select = self.__cursor.fetchone()

        if db_select is not None:
            res = db_select
        else:
            res = ["Not found"]

        return res

    def joinQuery(self) -> list:
        print("[Вывести id и имена подозреваемых, с которыми детектив detective_id имел дело]")

        detective_id = int(input("Введите id детектива (от 3000 до 4000): "))

        sql = """
                 select s.suspect_id, s.full_name
                 from crimes c join suspects_crimes sc on c.crime_id = sc.crime_id
                               join suspects s on sc.suspect_id = s.suspect_id
                 where detective = %s
                 order by s.suspect_id
              """ % detective_id

        self.__cursor.execute(sql)

        db_select = self.__cursor.fetchall()

        if db_select is not None:
            res = db_select
        else:
            res = ["Not found"]

        return res

    def CTEWindowFunctionQuery(self) -> list:
        print("[Для отдела department_id вывести \n"
              " - department_id, \n"
              " - detective_id, \n"
              " - количество решенных дел детектива, \n"
              " - среднее количество решенных дел по отделу, \n"
              " - разницу между количеством решенных дел детектива и средним]")

        department_id = int(input("Введите id отдела (от 1000 до 2000): "))

        sql = """
                 with cte as (
                    select department_id, detective_id, solved_crimes_num
                    from detectives join departments on department = department_id
                    where department_id = %s
                 )
                 select department_id, detective_id, solved_crimes_num,
                        (avg(solved_crimes_num) over(partition by department_id))::int avg_scn_by_dep,
                        (solved_crimes_num - (avg(solved_crimes_num) over(partition by department_id))::int) diff
                 from cte
              """ % department_id

        self.__cursor.execute(sql)

        db_select = self.__cursor.fetchall()

        if db_select is not None:
            res = db_select
        else:
            res = ["Not found"]

        return res

    def metaQuery(self) -> list:
        print("[Получить информацию о названиях и типах столбцов таблицы]")

        table_name = input("Введите название таблицы: ")

        sql = """
                 select column_name, data_type
                 from information_schema.columns
                 where table_name = '%s'
              """ % table_name

        self.__cursor.execute(sql)

        db_select = self.__cursor.fetchall()

        res = db_select
        if len(res) == 0:
            res = ["Table not found"]

        return res

    def scalarFuncQuery(self) -> list:
        print("[Получить максимальное число раскрытых преступлений в отделе department_id]")

        department_id = int(input("Введите id отдела (от 1000 до 2000): "))

        sql = """  
                 select get_max_solved_crimes_num_in_dep(%s) as max_solved_crimes_num
              """ % department_id

        self.__cursor.execute(sql)

        db_select = self.__cursor.fetchone()

        if db_select is not None:
            res = db_select
        else:
            res = ["Not found"]

        return res

    def tabularFuncQuery(self) -> list:
        print("[Получить информацию обо всех преступлениях подозреваемого suspect_id]")

        suspect_id = int(input("Введите id подозреваемого (от 5000 до 6000): "))

        sql = """
                 select *
                 from get_suspect_crimes(%s);
              """ % suspect_id

        self.__cursor.execute(sql)

        db_select = self.__cursor.fetchall()

        if db_select is not None:
            res = db_select
        else:
            res = ["Not found"]

        return res

    def storedProcCall(self) -> list:
        print("[Перевести детектива в другой отдел]")

        detective_id = int(input("Введите id детектива (3082, 3137, 3705, 3770, 3895): "))
        dep_to = int(input("Введите id отдела, в который необходимо перевести детектива (1000, 1003): "))

        sql = """
                 drop table if exists detectives_copy;
                 drop table if exists departments_copy;
                 
                 select * into temp detectives_copy
                 from detectives
                 where department between 1000 and 1005;
                 
                 select * into temp departments_copy
                 from departments
                 where department_id between 1000 and 1005;
              """

        self.__cursor.execute(sql)

        sql = """
                 select department
                 from detectives
                 where detective_id = %s
              """ % detective_id

        self.__cursor.execute(sql)
        db_select = self.__cursor.fetchone()

        if db_select is not None:
            dep_from = db_select[0]
        else:
            return ["Not found"]

        sql = """
                 call move_detective(%s, %s, %s);
              """ % (detective_id, dep_from, dep_to)

        self.__cursor.execute(sql)

        sql = """
                 select detective_id, full_name, department, department_size
                 from detectives_copy join departments_copy on department = department_id
                 where department_id between 1000 and 1005;
              """

        self.__cursor.execute(sql)

        db_select = self.__cursor.fetchall()

        if db_select is not None:
            res = db_select
        else:
            res = ["Not found"]

        return res

    def systemFuncQuery(self) -> list:
        print("[Сгенерировать ряд timestamp значений]")

        sql = """
                 select * 
                 from generate_series('2008-03-01 00:00'::timestamp,
                                      '2008-03-04 12:00', '10 hours');
              """

        self.__cursor.execute(sql)

        db_select = self.__cursor.fetchall()

        return db_select

    def createNewTable(self) -> list:
        print("[Создать таблицу преступлений за определенный период]")

        sql = """
                 create table if not exists crimes_for_period
                 (
                    crime_id integer primary key,
                    crime_date timestamp not null,
                    crime_place integer not null,
                    foreign key (crime_place) references places(place_id),
                    crime_type varchar(20) not null,
                    detective integer not null,
                    foreign key (detective) references detectives(detective_id)
                )
              """

        self.__cursor.execute(sql)

        return ["Done"]

    def insertIntoNewTable(self) -> list:
        print("[Заполнить таблицу преступлений за период]")

        date_from = input("Введите дату начала периода (формат: yyyy-mm-dd): ")
        date_to = input("Введите дату окончания периода (формат: yyyy-mm-dd): ")

        sql = """
                 select count(1)
                 from information_schema.tables
                 where table_name like 'crimes_for_period';
              """

        self.__cursor.execute(sql)
        db_select = self.__cursor.fetchall()

        if db_select[0][0] == 0:
            return ["Not found"]

        sql = """
                 delete from crimes_for_period;
                 
                 insert into crimes_for_period(crime_id, crime_date, crime_place, crime_type, detective)
                 select * 
                 from crimes
                 where crime_date between '%s' and '%s'
              """ % (date_from, date_to)

        self.__cursor.execute(sql)

        return ["Done"]

