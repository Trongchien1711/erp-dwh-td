-- ============================================================
-- Override dbt's default schema name behaviour.
--
-- By default dbt generates:  {target.schema}_{custom_schema}
-- With this macro, if a custom schema is set it is used EXACTLY
-- as-is (e.g. +schema: mart  →  schema "mart", not "public_mart").
-- If no custom schema is defined, the target schema is used.
--
-- Docs: https://docs.getdbt.com/docs/build/custom-schemas
-- ============================================================

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
