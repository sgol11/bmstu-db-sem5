-- 1. Триггер AFTER

-- Обновление id места в таблице crimes, если этот id изменился в таблице places

drop table if exists places_copy;
drop table if exists crimes_copy;

select *
into temp places_copy
from places;

select *
into temp crimes_copy
from crimes;

create or replace function update_place_id()
returns trigger
as '
begin
    raise notice ''Old =  %'', old; 
    raise notice ''New =  %'', new;

    update crimes_copy
    set crime_place = new.place_id
    where crime_place = old.place_id;
	
    return new;
end;' 
language plpgsql;

create trigger update_place_id_trigger
after update on places_copy
for each row
execute procedure update_place_id();

select *
from places_copy
where place_id = 8826;

select * 
from crimes_copy
where crime_place = 8826;

update places_copy
set place_id = 9001
where place_id = 8826;


-- 2. Триггер INSTEAD OF

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


create or replace function soft_dismissal()
returns trigger
as '
begin
    raise info ''Old =  %'', old;
    raise info ''New =  %'', new;
	
    update detectives_copy
    set department = null
    where detective_id = old.detective_id;
	
    return new;
end;' 
language plpgsql;

create trigger soft_dismissal_trigger
instead of delete on detectives_view
for each row
execute procedure soft_dismissal();

select * 
from detectives_view
order by detective_id;

delete from detectives_view
where detective_id = 3000;
