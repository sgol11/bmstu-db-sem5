from py_linq import Enumerable
from suspect_obj import create_suspects


def request_1(suspects):
    # Подозреваемые из Лос-Анджелеса с количеством преступлений > 4

    result = suspects. \
        where(lambda x: x['home_town'] == 'Los Angeles' and x['crimes_num'] > 4). \
        order_by_descending(lambda x: x['crimes_num'])

    return result


def request_2(suspects):
    # Среднее количество преступлений среди преступников 20 лет

    result = suspects. \
        where(lambda x: x['age'] == 20). \
        avg(lambda x: x['crimes_num'])

    return result


def request_3(suspects):
    # Сумма преступлений по городам

    result = suspects. \
        group_by(key_names=['home_town'], key=lambda x: x['home_town']). \
        select(lambda y: {'key': y.key.home_town, 'crimes_sum': y.sum(lambda z: z['crimes_num'])}). \
        order_by(lambda x: x['key'])

    return result


def request_4(suspects):
    # Union подозреваемых из Канзаса с количеством преступлений больше 5
    # и из любого города с количеством преступлений > 8

    suspects1 = suspects.where(lambda x: x['home_town'] == 'Kansas City' and x['crimes_num'] > 5)
    suspects2 = suspects.where(lambda x: x['crimes_num'] > 8)

    result = Enumerable(suspects1).union(Enumerable(suspects2), lambda x: x).take(15)

    return result


def request_5(suspects):
    # Join преступников и преступлений

    crimes = Enumerable([{'crime_id': i, 'suspect': 5000 + i} for i in range(1, 5)])
    sc = suspects.join(crimes, lambda o_k: o_k['suspect_id'], lambda i_k: i_k['suspect'])

    return sc


def request_6(suspects):
    # Для первых 5 подозреваемых вывести фамилию, затем имя
    # (в таблице есть столбец full_name с порядком записи имя-фамилия)

    names = suspects.select(lambda x: x['full_name']).take(5)
    result = []
    for name in names:
        result.append(' '.join(Enumerable(name.split()).reverse()))

    return result


def task_1():
    suspects = Enumerable(create_suspects('suspects.csv'))

    print('\n1. Подозреваемые из Лос-Анджелеса с количеством преступлений > 4\n')
    for elem in request_1(suspects):
        print(elem)

    print(f'\n2. Среднее количество преступлений среди преступников 20 лет: {str(request_2(suspects))}')

    print('\n3. Сумма преступлений по городам\n')
    for elem in request_3(suspects):
        print(elem)

    print('\n4. Union подозреваемых из Канзаса с количеством преступлений больше 5 \n'
          'и из любого города с количеством преступлений > 8\n')
    for elem in request_4(suspects):
        print(elem)

    print('\n5. Join преступников и преступлений\n')
    for elem in request_5(suspects):
        print(elem)

    print('\n6. Reverse имени-фамилии\n')
    for elem in request_6(suspects):
        print(elem)


if __name__ == '__main__':
    task_1()
