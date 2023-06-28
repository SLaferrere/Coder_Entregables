insert into
    laferreresantiago_coderhouse.UsersInformation
select
    preferable_prefix,
    full_name,
    country,
    province,
    email,
    age,
    cast(date_of_birth as date),
    cellphone,
    nationality,
    case
        when date_part(month, cast(date_of_birth as date)) = date_part(month, current_date) then 'Y'
        else 'N'
    end as send_congratulations
from
    laferreresantiago_coderhouse.Temp_UsersInformation u