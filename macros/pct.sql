{% macro pct(numerator, denominator, decimals=2) -%}
round(
    100.0 * ({{ numerator }}) / nullif(({{ denominator }}), 0),
    {{ decimals }}
)
{%- endmacro %}
