{% macro create_prediction(credit_status) %}
    
    case
        when {{ credit_status }} = 0 then 'Good'
        when {{ credit_status }} > 0 then 'Bad'
    end

{% endmacro %}