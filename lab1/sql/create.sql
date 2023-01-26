create database crime_investigation;

create table if not exists crimes
(
    crime_id integer not null,
    crime_date timestamp not null,
    crime_place integer not null,
    crime_type varchar(20) not null,
    detective integer not null
);

create table if not exists places
(
    place_id integer not null,
    latitude numeric(10, 6) not null,
    longitude numeric(10, 6) not null,
    place_type varchar(20) not null,
    place_object varchar(20) not null
);

create table if not exists suspects
(
    suspect_id integer not null,
    full_name varchar(40) not null,
    gender varchar(1) not null,
    age integer not null,
    home_town varchar(40) not null,
    crimes_num integer not null
);

create table if not exists detectives
(
    detective_id integer not null,
    full_name varchar(40) not null,
    experience integer not null,
    solved_crimes_num integer not null,
    department integer not null
);

create table if not exists departments
(
    department_id integer not null,
    address text not null,
    department_size integer not null,
    phone_number varchar(17) not null,
    email varchar(30) not null
);

create table if not exists suspects_crimes
(
    suspect_id integer not null,
    crime_id integer not null
);
