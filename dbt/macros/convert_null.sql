{% macro convert_null(null_value) %}
    
    coalesce({{ null_value }}, 'Unknown')

{% endmacro %}