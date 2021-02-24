{{ config(
    database=get_db_name('staging_db')
)}}

with base_chart as (

    select * from {{ source('external_sources', 'forecast_fy2022_base_chart_registration_calendarization') }}

)

, base_press as (

   select * from {{ source('external_sources', 'forecast_fy2022_base_press_registration_calendarization') }}

)

, base_lucidspark as (

   select * from {{ source('external_sources', 'forecast_fy2022_base_lucidspark_registration_calendarization') }}

)

, stretch_chart as (

   select * from {{ source('external_sources', 'forecast_fy2022_stretch_chart_registration_calendarization') }}

)

, stretch_press as (

    select * from {{ source('external_sources', 'forecast_fy2022_stretch_press_registration_calendarization') }}

)

, stretch_lucidspark as (

    select * from {{ source('external_sources', 'forecast_fy2022_stretch_lucidspark_registration_calendarization') }}

)

, chart_base_stretch as (

    select
         base_chart.*
        , stretch_chart.stretch
    from base_chart
    left join stretch_chart
        on base_chart.day = stretch_chart.day
        and base_chart.product = stretch_chart.product
        and base_chart.marketing_channel = stretch_chart.marketing_channel
        and base_chart.forecasting_sub_region = stretch_chart.forecasting_sub_region

)

, press_base_stretch as (

    select
         base_press.*
        , stretch_press.stretch
    from base_press
    left join stretch_press
        on base_press.day = stretch_press.day
        and base_press.product = stretch_press.product
        and base_press.marketing_channel = stretch_press.marketing_channel
        and base_press.forecasting_sub_region = stretch_press.forecasting_sub_region

)

, lucidspark_base_stretch as (

    select
         base_lucidspark.*
        , stretch_lucidspark.stretch
    from base_lucidspark
    left join stretch_lucidspark
        on base_lucidspark.day = stretch_lucidspark.day
        and base_lucidspark.product = stretch_lucidspark.product
        and base_lucidspark.marketing_channel = stretch_lucidspark.marketing_channel
        and base_lucidspark.forecasting_sub_region = stretch_lucidspark.forecasting_sub_region

)

, final as (

    select * from chart_base_stretch

    union all

    select * from press_base_stretch

    union all

    select * from lucidspark_base_stretch

)

select * from final