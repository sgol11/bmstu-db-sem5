-- select * from pg_language;
-- create extension if not exists plpython3u;


-- 1) Определяемая пользователем скалярная функция CLR

-- Максимальное количество раскрытых преступлений в отделе c department_id = department_num

drop function if exists get_max_solved_crimes_num_in_dep_clr(int);

create or replace function get_max_solved_crimes_num_in_dep_clr(department_num int = 1000)
returns int as 
$$
    res = plpy.execute(f"select max(solved_crimes_num) as max_num \
                         from departments join detectives on department_id = department \
                         where department_id = {department_num};")
    if res:
        return res[0]['max_num']                    
$$
language plpython3u;

select * from get_max_solved_crimes_num_in_dep_clr(1100) as max_solved_crimes_num;


-- 2) Пользовательская агрегатная функция CLR

-- Количество преступлений, совершенных в place_object

drop function if exists count_crimes;
drop aggregate if exists place_object_crimes(varchar);

create or replace function count_crimes(a int, place varchar)
returns int
as 
$$
res = plpy.execute(f"select * from crimes join places on crime_place = place_id;")
cnt = 0
				     
for i in res:
    if i["place_object"] == place:
        cnt += 1
		
return cnt
$$ 
language plpython3u;

create aggregate place_object_crimes(varchar)
(
    sfunc = count_crimes,
    stype = int
);

select place_object_crimes('Underground');


-- 3) Определяемая пользователем табличная функция CLR

-- Все преступления подозреваемого с suspect_id = suspect_num

drop function if exists get_suspect_crimes_clr;

create or replace function get_suspect_crimes_clr(suspect_num int)
returns table
(
    crime_id int, 
    crime_date timestamp, 
    crime_place int, 
    crime_type varchar, 
    detective int
)
as $$
buf = plpy.execute(f"select c.*, sc.suspect_id \
                     from crimes c join suspects_crimes sc on c.crime_id = sc.crime_id \
                     order by crime_id;")
res = []

for elem in buf:
    if elem["suspect_id"] == suspect_num:
        res.append(elem)
	    
return res
$$ 
language plpython3u;

select * from get_suspect_crimes_clr(5100);


-- 4) Хранимая процедура

-- Перевод детектива в другой отдел

drop table if exists detectives_copy;
drop table if exists departments_copy;
drop procedure if exists move_detective_clr;

select * into temp detectives_copy
from detectives
where department between 1000 and 1005;

select * into temp departments_copy
from departments
where department_id between 1000 and 1005;

create or replace procedure move_detective_clr
(
    detective_arg int,
    prev_dep_arg int,
    new_dep_arg int
)
as 
$$
plan = plpy.prepare("""update detectives_copy
                       set department = $2
                       where detective_id = $1""", ['integer', 'integer'])

rv = plpy.execute(plan, [detective_arg, new_dep_arg])

plan = plpy.prepare("""update departments_copy
                       set department_size = department_size - 1
                       where department_id = $1""", ['integer'])

rv = plpy.execute(plan, [prev_dep_arg])

plan = plpy.prepare("""update departments_copy
                       set department_size = department_size + 1
                       where department_id = $1""", ['integer'])

rv = plpy.execute(plan, [new_dep_arg])
$$ 
language plpython3u;

select detective_id, full_name, department, department_size
from detectives_copy join departments_copy on department = department_id
where department_id between 1000 and 1005;

call move_detective_clr(3705, 1000, 1003);


-- 5) Триггер CLR

-- Замена увольнения детектива (удаления из таблицы) на установку null в поле department_id

drop table if exists detectives_copy;

select *
into temp detectives_copy 
from detectives;

alter table detectives_copy alter column department drop not null;
	
drop view if exists detectives_view;

create view detectives_view as
    select *
    from detectives_copy;


create or replace function soft_dismissal_clr()
returns trigger
as 
$$
old_id = TD["old"]["detective_id"]
rv = plpy.execute(f"update detectives_copy \
                    set department = null \
                    where detective_id = {old_id}")

return TD["new"]
$$
language plpython3u;

create trigger soft_dismissal_trigger_clr
instead of delete on detectives_view
for each row
execute procedure soft_dismissal_clr();

select * 
from detectives_view
order by detective_id;

delete from detectives_view
where detective_id = 3000;


-- 6) Определяемый пользователем тип данных CLR

-- Пользовательский тип: географические координаты
-- Функция: получить координаты места, в котором совершено преступление crime_id

drop type if exists coords_t cascade;
drop function if exists get_crime_coords cascade;

create type coords_t as 
(
    latitude numeric,
    longitude numeric
);

create or replace function get_crime_coords(crime_id int)
returns coords_t
as
$$
query = "                                         \
select latitude, longitude                        \
from places join crimes on place_id = crime_place \
where crime_id = $1;"

plan = plpy.prepare(query, ["integer"])
	                     
res = plpy.execute(plan, [crime_id])

if (res.nrows()):
    return (res[0]["latitude"], res[0]["longitude"])
    
$$ language plpython3u;

select * 
from get_crime_coords(10);
