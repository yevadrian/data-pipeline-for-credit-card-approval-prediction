{% macro convert_binary(binary_value) %}
    
    case
        when {{ binary_value }} = 1 then 'Yes'
        when {{ binary_value }} = 0 then 'No'
    end

{% endmacro %}