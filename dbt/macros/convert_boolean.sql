{% macro convert_boolean(boolean_value) %}
    
    case
        when {{ boolean_value }} = 'Y' then 'Yes'
        when {{ boolean_value }} = 'N' then 'No'
    end

{% endmacro %}