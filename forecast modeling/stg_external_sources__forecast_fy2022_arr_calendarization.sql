{{ config(
    database=get_db_name('staging_db')
)}}

with base_chart as (

    select * from {{ source('external_sources', 'forecast_fy2022_base_chart_arr_calendarization') }}

)

, base_press as (

   select * from {{ source('external_sources', 'forecast_fy2022_base_press_arr_calendarization') }}

)

, base_lucidspark as (

   select * from {{ source('external_sources', 'forecast_fy2022_base_lucidspark_arr_calendarization') }}

)

, stretch_chart as (

   select * from {{ source('external_sources', 'forecast_fy2022_stretch_chart_arr_calendarization') }}

)

, stretch_press as (

   select * from {{ source('external_sources', 'forecast_fy2022_stretch_press_arr_calendarization') }}

)

, stretch_lucidspark as (

   select * from {{ source('external_sources', 'forecast_fy2022_stretch_lucidspark_arr_calendarization') }}

)

, chart_base_stretch as (

    select
          base_chart.day
        , base_chart.product
        , base_chart.day_attributed_level_group as grouping
        , lower(
            case base_chart.day_change_type
                when 'End' then 'Total'
                else base_chart.day_change_type
            end
          ) as arr_type
        , base_chart.forecasting_sub_region
        , base_chart.base
        , stretch_chart.stretch
    from base_chart
    left join stretch_chart
        on base_chart.day = stretch_chart.day
        and base_chart.product = stretch_chart.product
        and base_chart.day_attributed_level_group = stretch_chart.day_attributed_level_group
        and base_chart.day_change_type = stretch_chart.day_change_type
        and base_chart.forecasting_sub_region = stretch_chart.forecasting_sub_region
    where base_chart.day_change_type != 'Net Product Changes'

)

, press_base_stretch as (

    select
          base_press.day
        , base_press.product
        , base_press.day_attributed_level_group as grouping
        , lower(
            case base_press.day_change_type
                when 'End' then 'Total'
                else base_press.day_change_type
            end
          ) as arr_type
        , 'Global' as forecasting_sub_region
        , base_press.base
        , stretch_press.stretch
    from base_press
    left join stretch_press
        on base_press.day = stretch_press.day
        and base_press.product = stretch_press.product
        and base_press.day_attributed_level_group = stretch_press.day_attributed_level_group
        and base_press.day_change_type = stretch_press.day_change_type
        and base_press.forecasting_sub_region = stretch_press.forecasting_sub_region
    where base_press.day_change_type != 'Net Product Changes'

)

, lucidspark_base_stretch as (

    select
          base_lucidspark.day
        , base_lucidspark.product
        , base_lucidspark.day_attributed_level_group as grouping
        , lower(
            case base_lucidspark.day_change_type
                when 'End' then 'Total'
                else base_lucidspark.day_change_type
            end
          ) as arr_type
        , base_lucidspark.forecasting_sub_region
        , base_lucidspark.base
        , stretch_lucidspark.stretch
    from base_lucidspark
    left join stretch_lucidspark
        on base_lucidspark.day = stretch_lucidspark.day
        and base_lucidspark.product = stretch_lucidspark.product
        and base_lucidspark.day_attributed_level_group = stretch_lucidspark.day_attributed_level_group
        and base_lucidspark.day_change_type = stretch_lucidspark.day_change_type
        and base_lucidspark.forecasting_sub_region = stretch_lucidspark.forecasting_sub_region
    where base_lucidspark.day_change_type != 'Net Product Changes'

)

, base_stretch as (

    select * from chart_base_stretch

    union all

    select * from press_base_stretch

    union all

    select * from lucidspark_base_stretch

)

select * from base_stretch