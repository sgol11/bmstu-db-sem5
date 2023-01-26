-- 1. Хранимая процедура с параметрами

-- Перевод детектива в другой отдел

drop table if exists detectives_copy;
drop table if exists departments_copy;

select * into temp detectives_copy
from detectives
where department between 1000 and 1005;

select * into temp departments_copy
from departments
where department_id between 1000 and 1005;

create or replace procedure move_detective
(
    detective_arg int,
    prev_dep_arg int,
    new_dep_arg int
)
as '
begin
    update detectives_copy
    set department = new_dep_arg
    where detective_id = detective_arg;

    update departments_copy
    set department_size = department_size - 1
    where department_id = prev_dep_arg;

    update departments_copy
    set department_size = department_size + 1
    where department_id = new_dep_arg;
end;' 
language plpgsql;

select detective_id, full_name, department, department_size
from detectives_copy join departments_copy on department = department_id
where department_id between 1000 and 1005;

call move_detective(3705, 1000, 1003);


-- 2. Рекурсивная хранимая процедура

-- Сокращение численности каждого k-го отдела для n отделов, начиная с dep_id, на reduced_size
-- (reduced_size до 100)

drop table if exists departments_copy;

select department_id, department_size 
into temp departments_copy
from departments 
order by department_id;

drop procedure if exists reduce_department_size(int, int, int, int, int);

create or replace procedure reduce_department_size(cur int, dep_id int, k int, n int, reduced_size int)
as '
begin
    if cur <= n and reduced_size <= 100 then

        update departments_copy
        set department_size = department_size - reduced_size
        where department_id = dep_id;
	
        call reduce_department_size(cur + 1, dep_id + k, k, n, reduced_size);
		
    end if;
end;' 
language plpgsql;

select *
from departments_copy
order by department_id;

call reduce_department_size(1, 1000, 2, 5, 10);


-- 3. Хранимая процедура с курсором

-- Снятие обвинений с подозреваемых в деле crime_arg

drop table if exists crimes_copy;
drop table if exists suspects_crimes_copy;
drop table if exists suspects_copy;

select *
into temp crimes_copy
from crimes
where crime_id between 700 and 800;

select *
into temp suspects_crimes_copy
from suspects_crimes
where crime_id between 700 and 800;

select *
into temp suspects_copy
from suspects;


create or replace procedure drop_charges(crime_arg int)
as '
declare
    myCursor cursor for
        select suspect_id
        from suspects_crimes_copy
        where crime_id = crime_arg;

    cursor_suspect int;
	
begin
    open myCursor;
	
    loop
        fetch myCursor into cursor_suspect;
        exit when not found;
		
        update suspects_copy
        set crimes_num = crimes_num - 1
        where suspects_copy.suspect_id = cursor_suspect;
    end loop;

    close myCursor;

    delete from suspects_crimes_copy
    where crime_id = crime_arg;
end;'
language  plpgsql;

drop table if exists acquitted_suspects;

select suspect_id
into temp acquitted_suspects
from suspects_crimes_copy
where crime_id = 787
order by suspect_id;

select suspect_id, crimes_num 
from suspects_copy
where suspect_id in (select suspect_id from acquitted_suspects)
order by suspect_id;

select *
from crimes_copy
where crime_id = 787;

select crime_id, suspect_id
from suspects_crimes_copy
where crime_id = 787;

call drop_charges(787);


-- 4. Хранимая процедура доступа к метаданным

-- Информация о типах столбцов

drop procedure if exists get_table_meta(varchar);

create or replace procedure get_table_meta(my_table_name varchar)
as '
declare
    info record;
begin
    for info in
        select column_name, data_type
        from information_schema.columns
        where table_name = my_table_name
    loop
        raise info ''info = % '', info;
    end loop;
end;' 
language plpgsql;

call get_table_meta('crimes');


select pga.attname, pgt.typname
from pg_catalog.pg_attribute pga join pg_catalog.pg_class pgc on pga.attrelid = pgc.oid
                                 join pg_type pgt on pga.atttypid = pgt.oid
where pgc.relname = 'suspects';


-- Информация о размерах

drop procedure if exists get_table_size(varchar);

create or replace procedure get_table_size(my_table_name varchar)
as '
declare
    s record;
begin
	for s in
		select my_table_name, 
		pg_relation_size(my_table_name) as size 
		from information_schema.tables
		where table_name = my_table_name
	loop
    	raise info ''size = % '', s;
	end loop;
end;' 
language plpgsql;

call get_table_size('crimes');


