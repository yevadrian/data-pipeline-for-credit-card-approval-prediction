{% macro convert_gender(code_gender) %}
    
    case
        when {{ code_gender }} = 'M' then 'Male'
        when {{ code_gender }} = 'F' then 'Female'
    end

{% endmacro %}