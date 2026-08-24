{#
  Use the custom +schema value verbatim as the target database, instead of
  dbt's default "<target_schema>_<custom>" concatenation. This gives clean
  catalog databases: silver.laps, gold.fact_driver_season_summary — exactly the
  names an analyst or BI tool queries.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
