{{ config(
    database=get_db_name('staging_db') 
)}}

with base_chart as (

    select * from {{ source('external_sources', 'forecast_fy2022_base_chart_subs_by_sku_calendarization') }}

)

, stretch_chart as (

   select * from {{ source('external_sources', 'forecast_fy2022_stretch_chart_subs_by_sku_calendarization') }}

)

, base_lucidspark as (

    select * from {{ source('external_sources', 'forecast_fy2022_base_lucidspark_subs_by_sku_calendarization') }}

)

, stretch_lucidspark as (

   select * from {{ source('external_sources', 'forecast_fy2022_stretch_lucidspark_subs_by_sku_calendarization') }}

)

, chart_base_stretch as (

    select
          base_chart.day
        , base_chart.product
        , base_chart.day_attributed_level_group as grouping
        , base_chart.metric
        , base_chart.day_attributed_sales_group
        , base_chart.forecasting_sub_region
        , base_chart.base
        , stretch_chart.stretch
    from base_chart
    left join stretch_chart
        on base_chart.day = stretch_chart.day
        and base_chart.product = stretch_chart.product
        and base_chart.day_attributed_level_group = stretch_chart.day_attributed_level_group
        and base_chart.metric = stretch_chart.metric
        and base_chart.day_attributed_sales_group = stretch_chart.day_attributed_sales_group
        and base_chart.forecasting_sub_region = stretch_chart.forecasting_sub_region

)

, lucidspark_base_stretch as (

    select
          base_lucidspark.day
        , base_lucidspark.product
        , base_lucidspark.day_attributed_level_group as grouping
        , base_lucidspark.metric
        , base_lucidspark.day_attributed_sales_group
        , base_lucidspark.forecasting_sub_region
        , base_lucidspark.base
        , stretch_lucidspark.stretch
    from base_lucidspark
    left join stretch_lucidspark
        on base_lucidspark.day = stretch_lucidspark.day
        and base_lucidspark.product = stretch_lucidspark.product
        and base_lucidspark.day_attributed_level_group = stretch_lucidspark.day_attributed_level_group
        and base_lucidspark.metric = stretch_lucidspark.metric
        and base_lucidspark.day_attributed_sales_group = stretch_lucidspark.day_attributed_sales_group
        and base_lucidspark.forecasting_sub_region = stretch_lucidspark.forecasting_sub_region

)

, unions as (

    select *
    from chart_base_stretch

    union all

    select *
    from lucidspark_base_stretch

)

select * from unions