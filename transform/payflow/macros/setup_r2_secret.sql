{% macro setup_r2_secret() %}
  CREATE OR REPLACE PERSISTENT SECRET r2_payflow (
    TYPE S3,
    KEY_ID '{{ env_var("R2_ACCESS_KEY_ID") }}',
    SECRET '{{ env_var("R2_SECRET_ACCESS_KEY") }}',
    ENDPOINT '{{ env_var("R2_ENDPOINT") | replace("https://", "") }}',
    URL_STYLE 'path',
    REGION 'us-east-1',
    USE_SSL true
  );
{% endmacro %}
