{% macro convert_employed(days_employed) %}
    
    case
        when {{ days_employed }} < 0 then days_employed / -365
        when {{ days_employed }} > 0 then 0
    end

{% endmacro %}