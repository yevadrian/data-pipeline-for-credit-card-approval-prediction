{% macro convert_status(credit_status) %}
    
    case
        when {{ credit_status }} = 'X' then 0
        when {{ credit_status }} = 'C' then 0
        when {{ credit_status }} = '0' then 0
        when {{ credit_status }} = '1' then 1
        when {{ credit_status }} = '2' then 2
        when {{ credit_status }} = '3' then 3
        when {{ credit_status }} = '4' then 4
        when {{ credit_status }} = '5' then 5
    end

{% endmacro %}