{{
    generate_references(
          'stg_external_sources__calendar'
        , 'account_product_month'
        , 'stg_prod__levels'
    )
}}

, first_arr_dimensions as (

    select
        distinct
          first_arr_month as cohort_start_month
        , product
        , {{categorize_first_arr_by_band('first_arr_usd')}} as arr_band
        , first_arr_level_term as first_level_term
        , first_arr_level_group as first_level_group
        , forecasting_sub_region
        , current_account_owner_domain_type
        , sfdc_account_segment
        , null as level_term_before_enterprise
        , null as level_group_before_enterprise
    from account_product_month
    where first_day_of_month = first_arr_month

)

, first_enterprise_dimensions as (

    select
        distinct
          apm.first_enterprise_month as cohort_start_month
        , apm.product
        , {{categorize_first_enterprise_by_band('first_enterprise_arr_usd')}} as arr_band
        , apm.first_enterprise_level_term as first_level_term
        , apm.first_enterprise_level_group as first_level_group
        , apm.forecasting_sub_region
        , apm.current_account_owner_domain_type
        , apm.sfdc_account_segment
        , spl.term as level_term_before_enterprise
        , spl.grouping as level_group_before_enterprise
    from account_product_month as apm
    left join stg_prod__levels as spl
    on apm.level_id_before_enterprise = spl.level_id
    where first_day_of_month = first_enterprise_month

)

, months as (

    select
          first_day_of_month
        , date_trunc(month, last_day_of_fiscal_quarter) = first_day_of_month as is_quarterly_row
    from stg_external_sources__calendar
    where date = first_day_of_month
    and date < current_timestamp

)

, monthly_scaffold_first_arr as (

    select
          first_arr_dimensions.*
        , months.first_day_of_month
        , months.is_quarterly_row
    from first_arr_dimensions
    inner join months as months on first_arr_dimensions.cohort_start_month <= months.first_day_of_month

)

, monthly_scaffold_first_enterprise as (

    select
          first_enterprise_dimensions.*
        , months.first_day_of_month
        , months.is_quarterly_row
    from first_enterprise_dimensions
    inner join months as months on first_enterprise_dimensions.cohort_start_month <= months.first_day_of_month

)

, first_arr_aggs as (

    select
          first_arr_month
        , product
        , first_arr_band
        , first_arr_level_term
        , first_arr_level_group
        , forecasting_sub_region
        , current_account_owner_domain_type
        , sfdc_account_segment
        , first_day_of_month
        , null as level_term_before_enterprise
        , null as level_group_before_enterprise
        , sum(end_arr_usd) as ndr_usd
        , sum(iff(end_arr_usd <= first_arr_usd, end_arr_usd, first_arr_usd)) as gdr_usd
    from account_product_month
    where first_arr_month <= first_day_of_month
    group by
          first_arr_month
        , product
        , first_arr_band
        , first_arr_level_term
        , first_arr_level_group
        , forecasting_sub_region
        , current_account_owner_domain_type
        , sfdc_account_segment
        , first_day_of_month

)

, first_enterprise_aggs as (

    select
          apm.first_enterprise_month
        , apm.product
        , apm.first_enterprise_band
        , apm.first_enterprise_level_term
        , apm.first_enterprise_level_group
        , apm.forecasting_sub_region
        , apm.current_account_owner_domain_type
        , apm.sfdc_account_segment
        , apm.first_day_of_month
        , spl.term as level_term_before_enterprise
        , spl.grouping as level_group_before_enterprise
        , sum(apm.end_arr_usd) as ndr_usd
        , sum(iff(apm.end_arr_usd <= apm.first_enterprise_arr_usd, apm.end_arr_usd, apm.first_enterprise_arr_usd)) as gdr_usd
    from account_product_month as apm
    left join stg_prod__levels as spl
    on apm.level_id_before_enterprise = spl.level_id
    where first_enterprise_month <= first_day_of_month
    group by
          apm.first_enterprise_month
        , apm.product
        , apm.first_enterprise_band
        , apm.first_enterprise_level_term
        , apm.first_enterprise_level_group
        , apm.forecasting_sub_region
        , apm.current_account_owner_domain_type
        , apm.sfdc_account_segment
        , apm.first_day_of_month
        , level_group_before_enterprise
        , level_term_before_enterprise

)

, first_arr_joined as (

    select
          scaffold.*
        , datediff(month, scaffold.cohort_start_month, scaffold.first_day_of_month) as cohort_month
        , first_value(first_arr.ndr_usd) over (
            partition by
                  scaffold.cohort_start_month
                , scaffold.product
                , scaffold.arr_band
                , scaffold.first_level_term
                , scaffold.first_level_group
                , scaffold.forecasting_sub_region
                , scaffold.current_account_owner_domain_type
                , scaffold.sfdc_account_segment
            order by scaffold.first_day_of_month asc
        ) as cohort_start_arr
        , zeroifnull(first_arr.ndr_usd) as ndr_usd
        , zeroifnull(first_arr.gdr_usd) as gdr_usd
    from monthly_scaffold_first_arr as scaffold
    left join first_arr_aggs as first_arr
        on scaffold.cohort_start_month is not distinct from first_arr.first_arr_month
        and scaffold.product is not distinct from first_arr.product
        and scaffold.arr_band is not distinct from first_arr.first_arr_band
        and scaffold.first_level_term is not distinct from first_arr.first_arr_level_term
        and scaffold.first_level_group is not distinct from first_arr.first_arr_level_group
        and scaffold.forecasting_sub_region is not distinct from first_arr.forecasting_sub_region
        and scaffold.current_account_owner_domain_type is not distinct from first_arr.current_account_owner_domain_type
        and scaffold.sfdc_account_segment is not distinct from first_arr.sfdc_account_segment
        and scaffold.first_day_of_month is not distinct from first_arr.first_day_of_month

)

, first_enterprise_joined as (

    select
          scaffold.*
        , datediff(month, scaffold.cohort_start_month, scaffold.first_day_of_month) as cohort_month
        , first_value(first_ent.ndr_usd) over (
            partition by
                  scaffold.cohort_start_month
                , scaffold.product
                , scaffold.arr_band
                , scaffold.first_level_term
                , scaffold.first_level_group
                , scaffold.forecasting_sub_region
                , scaffold.current_account_owner_domain_type
                , scaffold.sfdc_account_segment
                , scaffold.level_group_before_enterprise
                , scaffold.level_term_before_enterprise
            order by scaffold.first_day_of_month asc
        ) as cohort_start_arr
        , zeroifnull(first_ent.ndr_usd) as ndr_usd
        , zeroifnull(first_ent.gdr_usd) as gdr_usd
    from monthly_scaffold_first_enterprise as scaffold
    left join first_enterprise_aggs as first_ent
        on scaffold.cohort_start_month is not distinct from first_ent.first_enterprise_month
        and scaffold.product is not distinct from first_ent.product
        and scaffold.arr_band is not distinct from first_ent.first_enterprise_band
        and scaffold.first_level_term is not distinct from first_ent.first_enterprise_level_term
        and scaffold.first_level_group is not distinct from first_ent.first_enterprise_level_group
        and scaffold.forecasting_sub_region is not distinct from first_ent.forecasting_sub_region
        and scaffold.current_account_owner_domain_type is not distinct from first_ent.current_account_owner_domain_type
        and scaffold.sfdc_account_segment is not distinct from first_ent.sfdc_account_segment
        and scaffold.first_day_of_month is not distinct from first_ent.first_day_of_month
        and scaffold.level_group_before_enterprise is not distinct from first_ent.level_group_before_enterprise
        and scaffold.level_term_before_enterprise is not distinct from first_ent.level_term_before_enterprise

)

, unioned as (

    select
          'first arr' as cohort_type
        , *
    from first_arr_joined

    union all

    select
          'first enterprise' as cohort_type
        , *
    from first_enterprise_joined

)

select * from unioned