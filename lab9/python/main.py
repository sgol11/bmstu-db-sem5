from time import time
import matplotlib.pyplot as plt
import psycopg2
import redis
import json
import threading
import configparser

N_REPEATS = 5


crime_types = {'Arson': 0, 'Burglary': 1, 'Domestic abuse': 2, 'Fraud': 3,
               'Murder': 4, 'Robbery': 5, 'Sexual harassment': 6}


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
        print("Error while connecting to database ❌", err)
        return

    print("PostgreSQL connection opened ✅")

    return con


# Написать запрос, получающий статистическую информацию на основе
# данных БД. Например, получение топ 10 самых покупаемых товаров или
# получение количества проданных деталей в каждом регионе.
# Количество преступлений каждого типа
def get_crimes_num_of_type(cur):
    redis_client = redis.Redis(host="localhost", port=6379, db=0)

    cache_value = redis_client.get("crimes_num_of_type")
    if cache_value is not None:
        redis_client.close()
        return json.loads(cache_value)

    cur.execute("select crime_type, count(1) as crimes_num "
                "from crimes "
                "group by crime_type "
                "order by crimes_num desc")
    res = cur.fetchall()

    redis_client.set("crimes_num_of_type", json.dumps(res))
    redis_client.close()

    return res


# 1. Приложение выполняет запрос каждые 5 секунд на стороне БД.
def task_02(cur, type):
    threading.Timer(5.0, task_02, [cur, type]).start()

    cur.execute(f"select count(1) as crimes_num \
                  from crimes \
                  where crime_type = '{type}'")

    result = cur.fetchone()

    return result


# 2. Приложение выполняет запрос каждые 5 секунд через Redis в качестве кэша.
def task_03(cur, type):
    threading.Timer(5.0, task_02, [cur, type]).start()

    redis_client = redis.Redis(host="localhost", port=6379, db=0)

    cache_value = redis_client.get(f"type{crime_types[type]}_count")
    if cache_value is not None:
        redis_client.close()
        return json.loads(cache_value)

    cur.execute(f"select count(1) as crimes_num \
                  from crimes \
                  where crime_type = '{type}'")

    result = cur.fetchone()
    data = json.dumps(result)
    redis_client.set(f"type{crime_types[type]}_count", data)
    redis_client.close()

    return result


def no_changes(cur):
    # threading.Timer(10.0, dont_do, [cur]).start()
    redis_client = redis.Redis(host="localhost", port=6379, db=0)

    t1 = time()
    cur.execute("select crime_type, count(1) as crimes_num "
                "from crimes "
                "group by crime_type "
                "order by crimes_num desc")
    t2 = time()

    result = cur.fetchall()

    data = json.dumps(result)
    cache_value = redis_client.get("type_cnt")
    if cache_value is not None:
        pass
    else:
        redis_client.set("type_cnt", data)

    t11 = time()
    redis_client.get("type_cnt")
    t22 = time()

    redis_client.close()

    return t2 - t1, t22 - t11


def insert_suspect(cur, con, index):
    redis_client = redis.Redis()
    # threading.Timer(10.0, ins_tour, [cur, con]).start()

    t1 = time()
    cur.execute(f"insert into suspects values({index}, 'John Black', 'm', 23, 'Atlanta', 2)")
    t2 = time()

    cur.execute(f"select * from suspects where suspect_id = {index}")
    result = cur.fetchall()
    data = json.dumps(result)

    t11 = time()
    redis_client.set(f"ins{index}", data)
    t22 = time()

    redis_client.close()

    con.commit()

    return t2 - t1, t22 - t11


def update_suspect(cur, con, index):
    redis_client = redis.Redis()
    # threading.Timer(10.0, upd_tour, [cur, con]).start()

    t1 = time()
    cur.execute(f"update suspects set crimes_num = 3 where suspect_id = {index}")
    t2 = time()

    cur.execute(f"select * from suspects where suspect_id = {index}")
    result = cur.fetchall()
    data = json.dumps(result)

    t11 = time()
    redis_client.set(f"upd{index}", data)
    t22 = time()

    redis_client.close()

    con.commit()

    return t2 - t1, t22 - t11


def delete_suspect(cur, con, index):
    redis_client = redis.Redis()
    # print("delete\n")
    # threading.Timer(10.0, del_tour, [cur, con]).start()

    t1 = time()
    cur.execute(f"delete from suspects where suspect_id = {index}")
    t2 = time()

    t11 = time()
    redis_client.delete(f"del{index}")
    t22 = time()

    redis_client.close()

    con.commit()

    return t2 - t1, t22 - t11


# Гистограммы
def task_04(cur, con):
    print("           | db       | redis")

    # simple
    t1 = 0
    t2 = 0
    for i in range(1000):
        b1, b2 = no_changes(cur)
        t1 += b1
        t2 += b2
    print("no changes | %.6f | %.6f" % (t1 / 1000, t2 / 1000))
    index = ["БД", "Redis"]
    values = [t1 / 1000, t2 / 1000]
    plt.bar(index, values)
    plt.title("Без изменения данных")
    plt.show()

    # insert
    t1 = 0
    t2 = 0
    for i in range(100):
        b1, b2 = insert_suspect(cur, con, 6001 + i)
        t1 += b1
        t2 += b2
    print("insert     | %.6f | %.6f" % (t1 / 100, t2 / 100))

    index = ["БД", "Redis"]
    values = [t1 / 100, t2 / 100]
    plt.bar(index, values)
    plt.title("При добавлении новых строк каждые 10 секунд")
    plt.show()

    # updata
    t1 = 0
    t2 = 0
    for i in range(100):
        b1, b2 = update_suspect(cur, con, 6001 + i)
        t1 += b1
        t2 += b2
    print("update     | %.6f | %.6f" % (t1 / 100, t2 / 100))

    index = ["БД", "Redis"]
    values = [t1 / 100, t2 / 100]
    plt.bar(index, values)
    plt.title("При изменении строк каждые 10 секунд")
    plt.show()

    # delete
    t1 = 0
    t2 = 0
    for i in range(100):
        b1, b2 = delete_suspect(cur, con, 6001 + i)
        t1 += b1
        t2 += b2
    print("delete     | %.6f | %.6f" % (t1 / 100, t2 / 100))

    index = ["БД", "Redis"]
    values = [t1 / 100, t2 / 100]
    plt.bar(index, values)
    plt.title("При удалении строк каждые 10 секунд")
    plt.show()


if __name__ == '__main__':

    config = configparser.ConfigParser()
    config.read("cfg.ini")
    con = connection(config["crimes_db"]["host"],
                     config["crimes_db"]["user"],
                     config["crimes_db"]["password"],
                     config["crimes_db"]["db_name"],
                     config["crimes_db"]["port"])
    cur = con.cursor()

    print("1. Количество преступлений каждого типа (задание 2)\n"
          "2. Приложение выполняет запрос каждые 5 секунд на стороне БД (задание 3.1)\n"
          "3. Приложение выполняет запрос каждые 5 секунд через Redis в качестве кэша (задание 3.2)\n"
          "4. Гистограммы (задание 3.3)\n"
          "0. Выход\n"
          )

    while True:
        c = int(input("\nВыбор: "))

        if c == 1:
            res = get_crimes_num_of_type(cur)

            for elem in res:
                print(elem)

        elif c == 2:
            crime_type = input("Тип преступления: ")

            res = task_02(cur, crime_type)

            for elem in res:
                print(elem)

        elif c == 3:
            crime_type = input("Тип преступления: ")

            res = task_03(cur, crime_type)

            for elem in res:
                print(elem)

        elif c == 4:
            task_04(cur, con)

        elif c == 0:
            break

        else:
            print("Ошибка\n")
            break

    cur.close()
    con.close()
