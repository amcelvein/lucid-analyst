{{
    generate_references(
          'arr_reporting_rollup_monthly_2020'
        , 'stg_external_sources__calendar'
        , 'stg_external_sources__countries'
    )
}}

, countries as (

    select distinct forecasting_sub_region, geo_region
    from stg_external_sources__countries

)

, months as (

    select distinct first_day_of_month as month
    from stg_external_sources__calendar
    where month < current_date()

)

, lucid_divisions as (

    select *
    from values ('chart'), ('lucidspark'), ('press'), ('lucid for education') as lucid_divisions(lucid_division)

)

, forecasting_sub_regions as (

    select distinct forecasting_sub_region
    from countries

    union

    select null as forecasting_sub_region

    union

    select 'Global' as forecasting_sub_region

)

, scaffolding as (

    select
          months.month
        , lucid_divisions.lucid_division
        , forecasting_sub_regions.forecasting_sub_region
    from months
    full outer join lucid_divisions
    full outer join forecasting_sub_regions

)

, cd_arr_rollup_month_actual as (

    select
          month
        , forecasting_sub_region
        , lucid_division
        , sum(month_actual) as metric
    from arr_reporting_rollup_monthly_2020 as arr
    where arr_type in ('churn', 'downgrade')
    group by month, forecasting_sub_region, lucid_division

)

, cd_arr_rollup_reforecast as (

    select
          month
        , forecasting_sub_region
        , lucid_division
        , sum(reforecast) as metric
    from arr_reporting_rollup_monthly_2020 as arr
    where arr_type in ('churn', 'downgrade')
    group by month, forecasting_sub_region, lucid_division

)

, nu_arr_rollup_month_actual as (

    select
          month
        , forecasting_sub_region
        , lucid_division
        , sum(month_actual) as metric
    from arr_reporting_rollup_monthly_2020 as arr
    where arr_type in ('new', 'upgrade')
    group by month, forecasting_sub_region, lucid_division

)

, nu_arr_rollup_reforecast as (

    select
          month
        , forecasting_sub_region
        , lucid_division
        , sum(reforecast) as metric
    from arr_reporting_rollup_monthly_2020 as arr
    where arr_type in ('new', 'upgrade')
    group by month, forecasting_sub_region, lucid_division

)

, total_arr_rollup_month_actual as (

    select
          month
        , forecasting_sub_region
        , lucid_division
        , sum(month_actual) as metric
    from arr_reporting_rollup_monthly_2020 as arr
    where arr_type = 'total'
    group by month, forecasting_sub_region, lucid_division

)

, total_arr_rollup_reforecast as (

    select
          month
        , forecasting_sub_region
        , lucid_division
        , sum(reforecast) as metric
    from arr_reporting_rollup_monthly_2020 as arr
    where arr_type = 'total'
    group by month, forecasting_sub_region, lucid_division

)


, final as (

    select
          scaffolding.month
        , scaffolding.lucid_division
        , scaffolding.forecasting_sub_region
        , countries.geo_region

        , cd_arr_rollup_month_actual.metric as churn_downgrade_month_actual_arr
        , lag(cd_arr_rollup_month_actual.metric, 12, 0) over (partition by scaffolding.forecasting_sub_region, scaffolding.lucid_division order by scaffolding.month asc) as prev_year_churn_downgrade_month_actual_arr
        , lag(cd_arr_rollup_month_actual.metric, 1, 0) over (partition by scaffolding.forecasting_sub_region, scaffolding.lucid_division order by scaffolding.month asc) as prev_month_churn_downgrade_month_actual_arr

        , cd_arr_rollup_reforecast.metric as churn_downgrade_reforecast_arr
        , lag(cd_arr_rollup_reforecast.metric, 12, 0) over (partition by scaffolding.forecasting_sub_region, scaffolding.lucid_division order by scaffolding.month asc) as prev_year_churn_downgrade_reforecast_arr
        , lag(cd_arr_rollup_reforecast.metric, 1, 0) over (partition by scaffolding.forecasting_sub_region, scaffolding.lucid_division order by scaffolding.month asc) as prev_month_churn_downgrade_reforecast_arr

        , nu_arr_rollup_month_actual.metric as new_upgrade_month_actual_arr
        , lag(nu_arr_rollup_month_actual.metric, 12, 0) over (partition by scaffolding.forecasting_sub_region, scaffolding.lucid_division order by scaffolding.month asc) as prev_year_new_upgrade_month_actual_arr
        , lag(nu_arr_rollup_month_actual.metric, 1, 0) over (partition by scaffolding.forecasting_sub_region, scaffolding.lucid_division order by scaffolding.month asc) as prev_month_new_upgrade_month_actual_arr

        , nu_arr_rollup_reforecast.metric as new_upgrade_reforecast_arr
        , lag(nu_arr_rollup_reforecast.metric, 12, 0) over (partition by scaffolding.forecasting_sub_region, scaffolding.lucid_division order by scaffolding.month asc) as prev_year_new_upgrade_reforecast_arr
        , lag(nu_arr_rollup_reforecast.metric, 1, 0) over (partition by scaffolding.forecasting_sub_region, scaffolding.lucid_division order by scaffolding.month asc) as prev_month_new_upgrade_reforecast_arr

        , total_arr_rollup_month_actual.metric as total_month_actual_arr
        , lag(total_arr_rollup_month_actual.metric, 12, 0) over (partition by scaffolding.forecasting_sub_region, scaffolding.lucid_division order by scaffolding.month asc) as prev_year_total_month_actual_arr
        , lag(total_arr_rollup_month_actual.metric, 1, 0) over (partition by scaffolding.forecasting_sub_region, scaffolding.lucid_division order by scaffolding.month asc) as prev_month_total_month_actual_arr

        , total_arr_rollup_reforecast.metric as total_reforecast_arr
        , lag(total_arr_rollup_reforecast.metric, 12, 0) over (partition by scaffolding.forecasting_sub_region, scaffolding.lucid_division order by scaffolding.month asc) as prev_year_total_reforecast_arr
        , lag(total_arr_rollup_reforecast.metric, 1, 0) over (partition by scaffolding.forecasting_sub_region, scaffolding.lucid_division order by scaffolding.month asc) as prev_month_total_reforecast_arr

    from scaffolding
    left join cd_arr_rollup_month_actual
        on cd_arr_rollup_month_actual.month = scaffolding.month
        and cd_arr_rollup_month_actual.forecasting_sub_region = scaffolding.forecasting_sub_region
        and cd_arr_rollup_month_actual.lucid_division = scaffolding.lucid_division
    left join cd_arr_rollup_reforecast
        on cd_arr_rollup_reforecast.month = scaffolding.month
        and cd_arr_rollup_reforecast.forecasting_sub_region = scaffolding.forecasting_sub_region
        and cd_arr_rollup_reforecast.lucid_division = scaffolding.lucid_division
    left join nu_arr_rollup_month_actual
        on nu_arr_rollup_month_actual.month = scaffolding.month
        and nu_arr_rollup_month_actual.forecasting_sub_region = scaffolding.forecasting_sub_region
        and nu_arr_rollup_month_actual.lucid_division = scaffolding.lucid_division
   left join nu_arr_rollup_reforecast
        on nu_arr_rollup_reforecast.month = scaffolding.month
        and nu_arr_rollup_reforecast.forecasting_sub_region = scaffolding.forecasting_sub_region
        and nu_arr_rollup_reforecast.lucid_division = scaffolding.lucid_division
   left join total_arr_rollup_month_actual
        on total_arr_rollup_month_actual.month = scaffolding.month
        and total_arr_rollup_month_actual.forecasting_sub_region = scaffolding.forecasting_sub_region
        and total_arr_rollup_month_actual.lucid_division = scaffolding.lucid_division
   left join total_arr_rollup_reforecast
        on total_arr_rollup_reforecast.month = scaffolding.month
        and total_arr_rollup_reforecast.forecasting_sub_region = scaffolding.forecasting_sub_region
        and total_arr_rollup_reforecast.lucid_division = scaffolding.lucid_division
    left join countries
        on countries.forecasting_sub_region = scaffolding.forecasting_sub_region

)

select * from final