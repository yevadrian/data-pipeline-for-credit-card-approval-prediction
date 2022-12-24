{% macro create_fact(dim) %}
    
    case {{ dim }}

        when 'Secondary / secondary special' then 1
        when 'Higher education' then 2
        when 'Incomplete higher' then 3
        when 'Lower secondary' then 4
        when 'Academic degree' then 5

        when 'Married' then 1
        when 'Civil marriage' then 2
        when 'Single / not married' then 3
        when 'Separated' then 4
        when 'Widow' then 5

        when 'House / apartment' then 1
        when 'Rented apartment' then 2
        when 'Co-op apartment' then 3
        when 'Office apartment' then 4
        when 'Municipal apartment' then 5
        when 'With parents' then 6

        when 'Pensioner' then 1
        when 'Student' then 2
        when 'Working' then 3
        when 'State servant' then 4
        when 'Commercial associate' then 5

        when 'Managers' then 1
        when 'Core staff' then 2
        when 'Accountants' then 3
        when 'Medicine staff' then 4
        when 'Low-skill Laborers' then 5
        when 'High skill tech staff' then 6
        when 'Private service staff' then 7
        when 'Drivers' then 8
        when 'HR staff' then 9
        when 'Laborers' then 10
        when 'Sales staff' then 11
        when 'Secretaries' then 12
        when 'Cooking staff' then 13
        when 'Realty agents' then 14
        when 'Cleaning staff' then 15
        when 'Security staff' then 16
        when 'Waiters/barmen staff' then 17
        when 'IT staff' then 18
        else 0

    end

{% endmacro %}