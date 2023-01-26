-- 1. Из таблиц базы данных, созданной в первой лабораторной работе, извлечь данные в JSON.

select row_to_json(c) result from crimes c;
select row_to_json(s) result from suspects_crimes s;
select row_to_json(s) result from suspects s;
select row_to_json(d) result from detectives d;
select row_to_json(d) result from departments d;
select row_to_json(p) result from places p;


-- 2. Выполнить загрузку и сохранение JSON файла в таблицу.
--    Созданная таблица после всех манипуляций должна соответствовать таблице
--    базы данных, созданной в первой лабораторной работе.

drop table if exists crimes_copy;

create table if not exists crimes_copy
(
    crime_id integer not null primary key,
    crime_date timestamp not null,
    crime_place integer not null,
    foreign key(crime_place) references places(place_id),
    crime_type varchar(20) not null,
    detective integer not null,
    foreign key (detective) references detectives(detective_id)
);

copy (select row_to_json(c) result from crimes c) to '/var/lib/postgresql/data/crimes_data/crimes.json';

drop table if exists crimes_import;

create table if not exists crimes_import(doc json);

copy crimes_import from 'crimes_data/crimes.json';

select * from crimes_import;

insert into crimes_copy
select (doc->>'crime_id')::int, (doc->>'crime_date')::timestamp, 
       (doc->>'crime_place')::int, doc->>'crime_type', (doc->>'detective')::int
from crimes_import;

select * from crimes_copy;


-- 3. Создать таблицу, в которой будет атрибут(-ы) с типом JSON, или
--    добавить атрибут с типом JSON к уже существующей таблице.
--    Заполнить атрибут правдоподобными данными с помощью команд INSERT
--    или UPDATE. 

drop table if exists detectives_json;

create table if not exists detectives_json(detective_data jsonb);

insert into detectives_json
select * from json_object('{detective_id, full_name, experience, solved_crimes_num, department}',
                          '{4010, "Sherlock Holmes", 40, 100, null}');
insert into detectives_json
select * from json_object('{detective_id, full_name, experience, solved_crimes_num, department}',
                          '{4011, "Hercule Poirot", 50, 86, null}');
insert into detectives_json
select * from json_object('{detective_id, full_name, experience, solved_crimes_num, department}',
                          '{4012, "Auguste Dupin", 15, 16, null}');

select * from detectives_json;


-- 4. Выполнить следующие действия:

--    1. Извлечь JSON фрагмент из JSON документа

select (doc->>'crime_id')::int crime_id, (doc->>'crime_date')::timestamp crime_date, 
       (doc->>'crime_place')::int crime_place, doc->>'crime_type' crime_type, 
       (doc->>'detective')::int detective
from crimes_import
where (doc->>'crime_date')::timestamp between '2003-01-01' and '2003-12-31';

select * from crimes_import, json_populate_record(null::crimes_copy, doc);

--    2. Извлечь значения конкретных узлов или атрибутов JSON документа

select (doc->>'crime_id')::int crime_id, (doc->>'crime_date')::timestamp crime_date, 
       doc->>'crime_type' crime_type
from crimes_import
where (doc->>'crime_date')::timestamp between '2003-01-01' and '2003-12-31';

--    3. Выполнить проверку существования узла или атрибута

drop function if exists is_key(json_to_check jsonb, key text);

create or replace function is_key(json_to_check jsonb, key text)
returns boolean
as 
$$
begin
    return json_to_check->key is not null;
end;
$$ 
language plpgsql;

-- Корректный случай
select is_key('{"crime_id": 1}', 'crime_id');
-- Некорректный случай
select is_key('{"crime_type": "Burglary"}', 'crime_id');

--    4. Изменить JSON документ

update detectives_json 
set detective_data = detective_data || '{"solved_crimes_num": 101}'::jsonb
where (detective_data->>'full_name') like 'Sherlock%';

select * from detectives_json;

--    5. Разделить JSON документ на несколько строк по узлам

select '[{"user_id": 0, "game_id": 1}, {"user_id":2, "game_id": 2}, {"user_id": 3, "game_id": 1}]'::jsonb;

select '[{"user_id": 0, "game_id": 1}, {"user_id":2, "game_id": 2}, {"user_id": 3, "game_id": 1}]'::json;

select jsonb_pretty(detective_data::jsonb) 
from detectives_json;
