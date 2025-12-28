{% macro amount_bucket(amount_expr) -%}
case
  when {{ amount_expr }} < 10 then 'micro'
  when {{ amount_expr }} < 50 then 'small'
  when {{ amount_expr }} < 200 then 'medium'
  when {{ amount_expr }} < 1000 then 'large'
  else 'xl'
end
{%- endmacro %}
