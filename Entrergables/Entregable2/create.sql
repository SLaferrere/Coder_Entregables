CREATE TABLE if not exists laferreresantiago_coderhouse.UsersInformation (
    preferable_prefix varchar(50) NOT NULL,
    full_name varchar(50) NOT NULL,
    country varchar(50) NOT NULL,
    province varchar(50) NOT NULL,
    email varchar(50) NOT NULL DISTKEY,
    age int NOT NULL,
    date_of_birth DATE NOT NULL,
    cellphone varchar(50) NOT NULL,
    nationality varchar(50) NOT NULL,
    send_congratulations varchar(1) NOT NULL
) compound sortkey(email)