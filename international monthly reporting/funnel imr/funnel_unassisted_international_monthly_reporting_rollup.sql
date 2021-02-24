{{
    generate_references(
          'recurly__subscriptions'
        , 'arr_reporting_rollup_monthly_2020'
        , 'stg_external_sources__calendar'
        , 'stg_external_sources__countries'
        , 'prod__users'
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

, products as (

    select *
    from values ('chart'), ('lucidspark'), ('press') as products(product)

)

, forecasting_sub_regions as (

    select distinct forecasting_sub_region
    from countries

    union

    select null as forecasting_sub_region

)

, scaffolding as (

    select
          months.month
        , products.product
        , forecasting_sub_regions.forecasting_sub_region
    from months
    full outer join products
    full outer join forecasting_sub_regions

)

, subscription_rollup as (

    select
            date_trunc(month, subscription_first_payment_at) as month
          , forecasting_sub_region
          , product
          , count(distinct uuid) as metric
          , 'num_subscriptions' as metric_name
    from recurly__subscriptions
    where subscription_first_payment_at is not null
    group by month, forecasting_sub_region, product

)

, bill_through_subscription_rollup as (

    select
            date_trunc(month, subscription_first_payment_at) as month
          , forecasting_sub_region
          , product
          , count(distinct iff(subscription_type = 'Trial Bill-Through', uuid, null)) as metric
          , 'num_bill_through_subscriptions' as metric_name
    from recurly__subscriptions
    where subscription_first_payment_at is not null
    group by month, forecasting_sub_region, product

)

, sign_up_rollup as (

    select
             date_trunc(month, activated_at) as month
           , forecasting_sub_region
           , product
           , count(distinct uuid) as metric
           ,  'num_signups' as metric_name
    from recurly__subscriptions
    group by month, forecasting_sub_region, product

)

, trials_rollup as (

    select
             date_trunc(month, trial_started_at) as month
           , forecasting_sub_region
           , product
           , count(distinct iff(trial_started_at is not null, uuid, null)) as metric
           , 'num_trials' as metric_name
    from recurly__subscriptions
    group by month, forecasting_sub_region, product

)

, chart_activation_rollup as (

    select
          date_trunc(month, chart_activated_at) as month
        , forecasting_sub_region
        , 'chart' as product
        , count(distinct user_id) as  metric
        , 'num_chart_activations' as metric_name
    from prod__users
    where chart_activated_at is not null
    group by month, forecasting_sub_region

)

, press_activation_rollup as (

    select
          date_trunc(month, press_activated_at) as month
        , forecasting_sub_region
        , 'press' as product
        , count(distinct user_id) as metric
        , 'num_press_activations' as metric_name
    from prod__users
    where press_activated_at is not null
    group by month, forecasting_sub_region

)

, lucidspark_activation_rollup as (

    select
          date_trunc(month, lucidspark_activated_at) as month
        , forecasting_sub_region
        , 'lucidspark' as product
        , count(distinct user_id) as  metric
        , 'num_lucidspark_activations' as metric_name
    from prod__users
    where lucidspark_activated_at is not null
    group by month, forecasting_sub_region

)

, unioned_rollups as (

    select *
    from subscription_rollup

    union

    select *
    from bill_through_subscription_rollup

    union

    select *
    from sign_up_rollup

    union

    select *
    from trials_rollup

    union

    select *
    from chart_activation_rollup

    union

    select *
    from press_activation_rollup

    union

    select *
    from lucidspark_activation_rollup

)

, final as (

    select
          unioned_rollups.*
        , countries.geo_region
        , lag(unioned_rollups.metric, 12, 0) over (partition by scaffolding.forecasting_sub_region, scaffolding.product, unioned_rollups.metric_name order by scaffolding.month asc) as prev_year_metric
        , lag(unioned_rollups.metric, 1, 0) over (partition by scaffolding.forecasting_sub_region, scaffolding.product, unioned_rollups.metric_name order by scaffolding.month asc) as prev_month_metric
    from scaffolding
    left join unioned_rollups
        on unioned_rollups.month = scaffolding.month
        and unioned_rollups.forecasting_sub_region = scaffolding.forecasting_sub_region
        and unioned_rollups.product = scaffolding.product
    left join countries
        on countries.forecasting_sub_region = scaffolding.forecasting_sub_region

)

select * from final