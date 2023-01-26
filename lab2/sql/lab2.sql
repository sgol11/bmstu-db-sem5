-- 1. Инструкция SELECT, использующая предикат сравнения
-- (Получить список детективов из 1060 отдела, имеющих опыт 
--  менее 20 лет, но успешно решивших более 20 дел)
select *
from detectives 
where department = 1060 and experience < 20 and solved_crimes_num > 20;

-- 2. Инструкция SELECT, использующая предикат BETWEENрек
-- (Получить список преступлений, совершенных в 2003 году)
select crime_id, crime_date 
from crimes 
where crime_date between '2003-01-01' and '2003-12-31'
order by crime_date;

-- 3. Инструкция SELECT, использующая предикат LIKE
-- (Получить список подозреваемых с фамилией Rodgers)
select full_name 
from suspects   
where full_name like '%Rodgers';

-- 4. Инструкция SELECT, использующая предикат IN с вложенным подзапросом
-- (Получить список преступлений (id и тип), в которых подозреваются 
--  люди из Сан-Диего)
select c.crime_id, crime_type
from crimes c join suspects_crimes sc on c.crime_id = sc.crime_id 
where suspect_id in (select suspect_id
                     from suspects 
                     where home_town = 'San Diego');
                     
 -- 5. Инструкция SELECT, использующая предикат EXISTS с вложенным подзапросом  
 -- (Получить список преступлений, в которых задействованы детективы с количеством раскрытых преступлений менее 5)
 select crime_id, crime_date, crime_type, detective
 from crimes 
 where exists (select detective_id 
               from detectives 
               where detectives.detective_id = crimes.detective and solved_crimes_num < 5);

-- 6. Инструкция SELECT, использующая предикат сравнения с квантором
-- (Получить список подозреваемых, которые старше любого подозреваемого из Саванны)
select *
from suspects 
where age > all(select age from suspects where home_town = 'Savannah');


-- 7. Инструкция SELECT, использующая агрегатные функции в выражениях столбцов
-- (Посчитать среднее количество раскрытых дел у детективов)
select avg(solved_crimes_num) as "actual avg", 
       sum(solved_crimes_num) / count(detective_id) as "calc avg"
from detectives;

-- 8. Инструкция SELECT, использующая скалярные подзапросы в выражениях столбцов
-- (Получить список имен детективов и количества подозреваемых для краж)
select crime_id, crime_date, crime_type, 
       (select full_name as detective
        from detectives
        where detective_id = crimes.detective),
       (select count(*) as suspects_num
        from suspects s join suspects_crimes sc on s.suspect_id = sc.suspect_id  
        where sc.crime_id = crimes.crime_id
        group by sc.crime_id)
from crimes
where crime_type = 'Robbery';

-- 9. Инструкция SELECT, использующая простое выражение CASE
-- (Вывести, сколько лет назад были совершены преступления, в качестве дополнительного столбца)
select *,
       case (extract(year from crime_date))
           when extract(year from now()) then 'This year'
           when extract(year from now()) - 1 then 'Last year'
           else (date_part('year', age(now(), crime_date)))::varchar(2) || ' years ago'
       end as "when"
from crimes;

-- 10. Инструкция SELECT, использующая поисковое выражение CASE
-- (Разделить детективов на джунов, мидлов и сеньоров в зависимости от количества решенных дел)
select *,
       case
           when solved_crimes_num < 15 then 'Junior'
           when solved_crimes_num < 40 then 'Middle'
           else 'Senior'
       end as "level"
from detectives;

-- 11. Создание новой временной локальной таблицы из результирующего набора данных инструкции SELECT
-- (Создать временную таблицу с количеством преступлений каждого типа по местам)
select place_object, crime_type, count(*) as crimes_num
into temp loc_crimes
from crimes join places on crime_place = place_id 
group by place_object, crime_type
order by place_object, crimes_num desc;

select * from loc_crimes;

-- 12. Инструкция SELECT, использующая вложенные коррелированные подзапросы в качестве производных 
--     таблиц в предложении FROM
-- (Получить список подозреваемых, совершивших максимальное для своего родного города количество преступлений)
select full_name, s.home_town, s.crimes_num 
from suspects s join (select home_town, max(crimes_num) as crimes_max
                      from suspects
                      group by home_town) ss 
                on s.home_town = ss.home_town and s.crimes_num = ss.crimes_max
order by s.home_town;
-- (Получить список дективов, работающих в маленьких отделах)
select full_name, department, department_size
from detectives d join (select department_id, department_size
                        from departments 
                        where department_size < 100) sd on d.department = sd.department_id;

-- 13. Инструкция SELECT, использующая вложенные подзапросы с уровнем вложенности 3
-- (Получить список преступлений, совершенных людьми из города с самым большим количеством преступников)
select c.crime_id, crime_type, full_name as suspect, home_town
from crimes c join suspects_crimes sc on c.crime_id = sc.crime_id 
              join suspects s on sc.suspect_id = s.suspect_id
where s.home_town = (select home_town
                     from suspects 
                     group by home_town 
                     having count(*) = (select max(suspects_num) as suspects_max
                                        from (select home_town, count(*) as suspects_num
                                              from suspects 
                                              group by home_town) ss));
                                                  
-- 14. Инструкция SELECT, консолидирующая данные с помощью предложения GROUP BY, но без предложения HAVING
-- (Получить список количества преступлений и даты последнего преступления по типам мест)
select place_type, count(*) as crimes_num, max(crime_date) as last_crime
from places join crimes on place_id = crime_place 
group by place_type;

-- 15. Инструкция SELECT, консолидирующая данные с помощью предложения GROUP BY и предложения HAVING.
-- (Получить список количества преступлений и даты последнего преступления по географическим объектам 
--  с условием, что последнее преступление на этом месте было совершено в 2022)
select place_object, count(*) as crimes_num, max(crime_date) as last_crime
from places join crimes on place_id = crime_place 
group by place_object
having extract(year from max(crime_date)) = 2022;

-- 16. Однострочная инструкция INSERT, выполняющая вставку в таблицу одной строки значений
insert into places(place_id, latitude, longitude, place_type, place_object)
values(10000, 80, -54, 'Outdoors', 'Forest');

-- 17. Многострочная инструкция INSERT, выполняющая вставку в таблицу результирующего набора 
--     данных вложенного подзапроса
insert into crimes(crime_id, crime_date, crime_place, crime_type, detective)
select (select max(crime_id) + 1
        from crimes),
       '2022-01-01',
       (select min(place_id)
        from places 
        where place_object = 'Garden'),
       'Murder',
       detective_id
from detectives 
where full_name = 'Perry Gibson';

-- 18. Простая инструкция UPDATE
update departments 
set department_size = department_size + 50
where department_id < (select min(department_id) + 100
                       from departments);
                      
-- 19. Инструкция UPDATE со скалярным подзапросом в предложении SET
update departments 
set department_size = (select max(department_size)
                       from departments
                       where department_id < (select max(department_id) - 100
                                              from departments))
where department_id = (select max(department_id)
                       from departments);
                      
-- 20. Простая инструкция delete
-- (После пункта 16)
delete from places
where place_id = 10000;

-- 21. Инструкция DELETE с вложенным коррелированным подзапросом в предложении WHERE
delete from suspects_crimes 
where crime_id in (select crime_id 
                   from crimes 
                   where detective in (select detective_id 
                                       from detectives left outer join crimes on detective_id = detective
                                       where experience = 0 and solved_crimes_num > 10));                  
delete from crimes
where detective in (select detective_id 
                    from detectives left outer join crimes on detective_id = detective
                    where experience = 0 and solved_crimes_num > 10);
                               
select *
from crimes 
where detective in (select detective_id 
                    from detectives left outer join crimes on detective_id = detective 
                    where experience = 0 and solved_crimes_num > 10);


-- 22. Инструкция SELECT, использующая простое обобщенное табличное выражение
-- (Получить максимальное число преступлений, совершенных людьми из одного города)
with cte as (
    select home_town, sum(crimes_num) as crimes_total
    from suspects 
    group by home_town
)
select max(crimes_total) as crimes_max
from cte;

-- 23. Инструкция SELECT, использующая рекурсивное обобщенное табличное выражение
with cte as (
    select *,
    dense_rank() over (partition by department order by experience desc, solved_crimes_num desc) as num_in_dep
    from detectives
)
select detective_id, full_name, experience, solved_crimes_num, department,
       (case
	    when num_in_dep = 1 then null
            when num_in_dep between 2 and 3 then (select detective_id
                                                  from cte c
                                                  where cte.department = c.department and num_in_dep = 1)
            else (select detective_id
                  from cte c
                  where cte.department = c.department and c.num_in_dep = cte.num_in_dep - 2)
        end) as boss
into temp detectives_with_bosses
from cte
order by detective_id;

select * from detectives_with_bosses;

with recursive boss_hierarchy (detective_id, full_name, department, boss, boss_level) as (

    select detective_id, full_name, department, boss, 0 as boss_hierarchy
    from detectives_with_bosses
    where boss is null
    
    union all 
    
    select db.detective_id, db.full_name, db.department, db.boss, boss_level + 1
    from detectives_with_bosses db join boss_hierarchy bh on db.boss = bh.detective_id
)
select *
from boss_hierarchy
order by detective_id;

-- 24. Оконные функции. Использование конструкций MIN/MAX/AVG OVER()
select detective_id, full_name, experience, department,
       avg(experience) over(partition by department) as avg_exp,
       min(experience) over(partition by department) as min_exp,
       max(experience) over(partition by department) as max_exp
from detectives;

-- 25. Оконные функции для устранения дублей
with duplicated as (
    select * 
    from suspects
    where age between 20 and 40
    union all
    select * 
    from suspects
    where age between 30 and 50
)
select *
from (select suspect_id, full_name, age, row_number() over (partition by suspect_id) as rn 
      from duplicated) dpl
where dpl.rn = 1;

