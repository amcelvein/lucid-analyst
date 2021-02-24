{% macro categorize_first_arr_by_band(arr_usd) %}
---real arr numbers are replaced with non proprietary placeholders

case
   when {{arr_usd}} < 'a' then '<a'
   when {{arr_usd}} < 'b' then '=a-b'
   when {{arr_usd}} < 'c' then '=b-c'
   when {{arr_usd}} >= 'c' then '>=c'
   when {{arr_usd}} is null then 'Null'
   else 'broken'
   end
{% endmacro %}