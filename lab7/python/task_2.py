from suspect_obj import Suspect
import psycopg2
import configparser

N = 5


def connection(host, user, password, db_name, port):
    try:
        con = psycopg2.connect(
            host=host,
            user=user,
            password=password,
            database=db_name,
            port=port
        )
    except Exception as err:
        print("Error while connecting to database", err)
        return

    print("PostgreSQL connection opened")

    return con


def create_workers_json(cur):
    # Создание JSON документа путем извлечения из таблиц базы данных

    with open('suspects.json', 'w') as f:
        cur.copy_to(f, 'suspects')

    query = '''
    create table if not exists suspects_json(
    suspect_id integer not null,
    full_name varchar(40) not null,
    gender varchar(1) not null,
    age integer not null,
    home_town varchar(40) not null,
    crimes_num integer not null
    );
    '''
    cur.execute(query)

    with open('suspects.json', 'r') as f:
        cur.copy_from(f, 'suspects_json')


def output_json(array):
    print(*array, sep='\n')


def read_table_json(cur, count=N):
    # Чтение из JSON документа

    cur.execute("select * from suspects_json")
    rows = cur.fetchmany(count)

    array = list()
    for elem in rows:
        array.append(Suspect(*elem))

    output_json(array)

    return array


def update_user(suspects, suspect_id):
    # Обновление JSON документа
    # (Увличение количества преступлений у преступника suspect_id на 1)

    for elem in suspects:
        if elem.suspect_id == suspect_id:
            elem.crimes_num += 1

    output_json(suspects)


def add_worker(suspects, suspect):
    # Добавление в JSON документ

    suspects.append(suspect)
    output_json(suspects)


def task_2():
    config = configparser.ConfigParser()
    config.read("cfg.ini")
    con = connection(config["crimes_db"]["host"],
                     config["crimes_db"]["user"],
                     config["crimes_db"]["password"],
                     config["crimes_db"]["db_name"],
                     config["crimes_db"]["port"])
    cur = con.cursor()

    create_workers_json(cur)

    print("\n1.Чтение из JSON документа:")
    suspects_array = read_table_json(cur)

    print("\n2.Обновление XML/JSON документа:")
    update_user(suspects_array, 5002)

    print("\n3.Добавление в XML/JSON документ:")
    add_worker(suspects_array, Suspect(6001, 'Jack the Ripper', 'm', 29, 'London', 5))

    cur.close()
    con.close()


if __name__ == '__main__':
    task_2()
