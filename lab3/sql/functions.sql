-- 1. Скалярная функция

-- Максимальное количество раскрытых преступлений в отделе c department_id = department_num

drop function if exists get_max_solved_crimes_num_in_dep(int);

create or replace function get_max_solved_crimes_num_in_dep(department_num int = 1000)
returns int as '
    select max(solved_crimes_num)
    from departments join detectives on department_id = department
    where department_id = department_num;' 
language sql;

select get_max_solved_crimes_num_in_dep(1100) as max_solved_crimes_num;

-- check
select department_id, MAX(solved_crimes_num)
from departments join detectives on department_id = department
group by department_id
order by department_id;


-- 2. Подставляемая табличная функция

-- Все преступления подозреваемого с suspect_id = suspect_num

drop function if exists get_suspect_crimes(int);

create or replace function get_suspect_crimes(suspect_num int = 5000) 
returns table (crime_id int, crime_date timestamp, crime_place int, crime_type varchar(20), detective int) as '
    select c.* 
    from crimes c join suspects_crimes sc on c.crime_id = sc.crime_id 
                  join suspects s on sc.suspect_id = s.suspect_id 
    where s.suspect_id = suspect_num' 
language sql;

select *
from get_suspect_crimes(5100);

-- check
select c.* 
from crimes c join suspects_crimes sc on c.crime_id = sc.crime_id 
              join suspects s on sc.suspect_id = s.suspect_id 
where s.suspect_id = 5100;


-- 3. Многооператорная табличная функция

-- Информация о количестве преступлений для подозреваемых из заданного возрастного диапазона

drop function if exists get_crimes_num_info(int, int);

create or replace function get_crimes_num_info(la int = 18, ra int = 61) 
	returns table (suspect_id int, 
                       age int,
                       crimes_diff int,
                       crimes_avg int,
                       crimes_min int, 
                       crimes_max int)
	language plpgsql
as 
$$
declare
    crimes_avg int;
    crimes_min int;
    crimes_max int;
begin 
    select avg(crimes_num::int)
    into crimes_avg
    from suspects
    where suspects.age between la and ra; 

    select min(crimes_num::int)
    into crimes_min
    from suspects
    where suspects.age between la and ra;  

    select max(crimes_num::int)
    into crimes_max
    from suspects
    where suspects.age between la and ra; 

    return query 
            select suspects.suspect_id, 
                   suspects.age,
                   suspects.crimes_num::int - crimes_avg AS crimes_diff, 
                   crimes_avg,
                   crimes_min, 
                   crimes_max
            from suspects
            where suspects.age between la and ra; 
end;
$$;

select *
from get_crimes_num_info(20, 20);


-- 4. Функция с рекурсивным ОТВ

-- Иерархия в заданном отделе

--------------------------------------------------------------------------------------------------------------
-- Создание таблички с боссами

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
--------------------------------------------------------------------------------------------------------------

drop function if exists dep_hierarchy(int);

create or replace function dep_hierarchy(dep_arg int)
returns table
(
    detective_id int, 
    full_name varchar(40), 
    department int, 
    boss int, 
    boss_level int
)
as '
    with recursive boss_hierarchy (detective_id, full_name, department, boss, boss_level) as (

        select detective_id, full_name, department, boss, 0 as boss_hierarchy
        from detectives_with_bosses
        where boss is null and department = dep_arg
    
        union all 
    
        select db.detective_id, db.full_name, db.department, db.boss, boss_level + 1
        from detectives_with_bosses db join boss_hierarchy bh on db.boss = bh.detective_id
    )

    select *
    from boss_hierarchy
    order by detective_id;' 
language sql;

select *
from dep_hierarchy(1000);


-- fibonacci just for fun

drop function if exists fib(int, int, int);

create or replace function fib(first_num int, second_num int, amount int)
returns table (fibonacci int) as '
    with recursive fibonacci_rec(iter, a, b) as 
    (
	select 1, first_num as a, second_num as b
        
        union all

        select iter + 1, b as a, a + b as b
        from fibonacci_rec 
        where iter + 1 <= amount
    )

    select a from fibonacci_rec;' 
language sql;

select *
from fib(1, 1, 10);

