from peewee import *
import configparser

config = configparser.ConfigParser()
config.read("cfg.ini")
con = PostgresqlDatabase(
    host=config["rk3"]["host"],
    user=config["rk3"]["user"],
    password=config["rk3"]["password"],
    database=config["rk3"]["db_name"],
    port=config["rk3"]["port"]
)


# Классы сущностей, моделирующие таблицы базы данных


class BaseModel(Model):
    class Meta:
        database = con


class Places(BaseModel):
    place_id = IntegerField(column_name='place_id', primary_key=True)
    latitude = FloatField(column_name='latitude')
    longitude = FloatField(column_name='longitude')
    place_type = CharField(column_name='place_type')
    place_object = CharField(column_name='place_object')

    class Meta:
        table_name = 'places'


class Suspects(BaseModel):
    suspect_id = IntegerField(column_name='suspect_id', primary_key=True)
    full_name = CharField(column_name='full_name')
    gender = CharField(column_name='gender')
    age = IntegerField(column_name='age')
    home_town = CharField(column_name='home_town')
    crimes_num = IntegerField(column_name='crimes_num')

    class Meta:
        table_name = 'suspects'


class Departments(BaseModel):
    department_id = IntegerField(column_name='department_id', primary_key=True)
    address = CharField(column_name='address')
    department_size = IntegerField(column_name='department_size')
    phone_number = CharField(column_name='phone_number')
    email = CharField(column_name='email')

    class Meta:
        table_name = 'departments'


class Detectives(BaseModel):
    detective_id = IntegerField(column_name='detective_id', primary_key=True)
    full_name = CharField(column_name='full_name')
    experience = IntegerField(column_name='experience')
    solved_crimes_num = IntegerField(column_name='solved_crimes_num')
    department = IntegerField(column_name='department')

    class Meta:
        table_name = 'detectives'


class Crimes(BaseModel):
    crime_id = IntegerField(column_name='crime_id', primary_key=True)
    crime_date = TimestampField(column_name='crime_date')
    crime_place = ForeignKeyField(Places)
    crime_type = CharField(column_name='crime_type')
    detective = IntegerField(column_name='detective')

    class Meta:
        table_name = 'crimes'


class SuspectsCrimes(BaseModel):
    suspect_id = IntegerField(column_name='suspect_id')
    crime_id = IntegerField(column_name='crime_id')

    class Meta:
        table_name = 'suspects_crimes'


def query_1():
    print("1. Однотабличный запрос на выборку")
    print('Детективы из 1804 отдела с количеством раскрытых дел > 10\n')

    query = Detectives.select(). \
        where(Detectives.department == 1804, Detectives.solved_crimes_num > 10). \
        order_by(Detectives.solved_crimes_num.desc())

    detectives_selected = query.dicts().execute()

    for elem in detectives_selected:
        print(elem)


def query_2():
    print("\n2. Многотабличный запрос на выборку")
    print("Топ годов по среднему количеству преступлений подозреваемых\n")

    query = Crimes.select(con.extract_date('year', Crimes.crime_date).alias('year'),
                          fn.AVG(Suspects.crimes_num).alias('avg_crimes_num')). \
        join(SuspectsCrimes, on=(Crimes.crime_id == SuspectsCrimes.crime_id)).\
        join(Suspects, on=(Suspects.suspect_id == SuspectsCrimes.suspect_id)).\
        group_by(con.extract_date('year', Crimes.crime_date)).\
        order_by(fn.AVG(Suspects.crimes_num).desc())

    u_b = query.dicts().execute()

    for elem in u_b:
        print(elem)


def print_last_suspects():
    print("\nПоследние 5 подозреваемых\n")
    query = Suspects.select().order_by(Suspects.suspect_id.desc()).limit(5)
    for elem in query.dicts().execute():
        print(elem)
    print()


def add_suspect(new_suspect_id, new_full_name, new_gender, new_age, new_home_town, new_crimes_num):
    try:
        with con.atomic() as txn:
            Suspects.create(suspect_id=new_suspect_id, full_name=new_full_name,
                            gender=new_gender, age=new_age,
                            home_town=new_home_town, crimes_num=new_crimes_num)
            print("Подозреваемый успешно добавлен")
    except Exception as e:
        print(e)
        txn.rollback()


def update_suspect(suspect_id_in):
    try:
        suspect = Suspects.get(Suspects.suspect_id == suspect_id_in)
        suspect.crimes_num = 0
        suspect.save()
        print("Количество преступлений подозреваемого обнулено")
    except Exception as e:
        print(e)


def del_suspect(suspect_id_in):
    try:
        suspect = Suspects.get(Suspects.suspect_id == suspect_id_in)
        suspect.delete_instance()
        print("Подозреваемый успешно удален из таблицы")
    except Exception as e:
        print(e)


def query_3():
    # 3. Три запроса на добавление, изменение и удаление данных в базе данных

    print("\n3. Три запроса на добавление, изменение и удаление данных в базе данных")

    print_last_suspects()

    add_suspect(6001, 'John Brown', 'm', 29, 'Orlando', 2)
    print_last_suspects()

    update_suspect(6001)
    print_last_suspects()

    del_suspect(6001)
    print_last_suspects()


def print_last_detectives():
    print("\nПоследние 5 детективов\n")
    query = Detectives.select().order_by(Detectives.detective_id.desc()).limit(5)
    for elem in query.dicts().execute():
        print(elem)
    print()


def add_detective(new_detective_id, new_full_name, new_experience, new_solved_crimes_num, new_department):
    try:
        with con.atomic() as txn:
            Detectives.create(detective_id=new_detective_id, full_name=new_full_name,
                              experience=new_experience, solved_crimes_num=new_solved_crimes_num,
                              department=new_department)
            print("Детектив успешно добавлен")
    except Exception as e:
        print(e)
        txn.rollback()


def query_4():
    # 4. Получение доступа к данным, выполняя только хранимую процедуру

    cursor = con.cursor()

    print("\n4. Получение доступа к данным, выполняя только хранимую процедуру\n")

    add_detective(4001, 'Mary Gold', 5, 10, 1000)
    print_last_detectives()

    cursor.execute('call move_detective(4001, 1000, 1001)')

    con.commit()

    print_last_detectives()

    cursor.close()


def task_3():
    global con

    query_1()
    query_2()
    query_3()
    query_4()

    con.close()


if __name__ == '__main__':
    task_3()
