{#
  By default dbt builds custom schemas as <target_schema>_<custom_schema>, e.g. BRONZE_SILVER.
  Our schemas already exist (created by Terraform) and TRANSFORMER_ROLE cannot create new ones,
  so use the custom schema name exactly as given: +schema: SILVER -> FINANCE_DEV.SILVER.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim | upper }}
    {%- endif -%}
{%- endmacro %}
