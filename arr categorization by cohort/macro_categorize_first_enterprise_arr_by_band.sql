{% macro categorize_first_enterprise_by_band(arr_usd) %}
---real arr numbers are replaced with non proprietary placeholders

case
   when {{arr_usd}} < 'x' then '<x'
   when {{arr_usd}} < 'y' then '=x-y'
   when {{arr_usd}} < 'z' then '=y-z'
   when {{arr_usd}} < 'xx' then 'z-xx'
   when {{arr_usd}} < 'yy' then '=xx-yy'
   when {{arr_usd}} < 'zz' then '=yy-zz'
   when {{arr_usd}} < 'aa' then '=zz-aa'
   when {{arr_usd}} >='aa' then '>=aa'
   when {{arr_usd}} is null then 'Null'
   else 'broken'
   end
{% endmacro %}