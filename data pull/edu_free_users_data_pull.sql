{{generate_references(
      'users_pii'
    , 'sfdc__account'
    , 'addedShapeToDocument'
    , 'mt__user_product_aggregations'
    , 'commented'
    , 'prod__documents'
    , 'user_storage_used'
    , 'c__user_integrations'
    , 'enteredPresentationMode'
    , 'restoredDocumentFromRevisionHistory'
)}}

, users as (

    select
         users.user_id
        , users.first_name
        , users.last_name
        , users.email
        , users.email_domain
        , users.domain_type
        , users.geo_city
        , users.geo_country_code
        , users.chart_persona
        , users.chart_subpersona
        , users.press_persona
        , account.billing_state
        , account.institution_type
    from users_pii as users
    left join sfdc__account as account
        on users.sfdc_account_id = account.sfdc_account_id
    where (chart_level_id = 28
    or press_level_id = 128
    or lucidspark_level_id = 238)

)

, used_shape_library as (

    select
         distinct
         user_id
        , shape_library_name
    from addedShapeToDocument
    where shape_library_name in ('mindmap', 'timeline', 'android', 'ios', 'ios7')

)

, documents_created as (

    select
         user_id
        , non_deleted_documents_created_all_time as number_of_non_deleted_chart_documents
    from mt__user_product_aggregations
    where non_deleted_documents_created_all_time > 3
    and product = 'chart'

)

, comments_given_by_user as (

    select
         user_id
        , count(*) as number_of_comments_given
    from commented
    group by user_id

)

, comments_recieved_by_user as (

    select
         doc.creator_id
        , count(*) as number_of_comments_recieved
    from commented as com
    inner join prod__documents as doc
        on com.document_id = doc.document_id
    where com.user_id = doc.creator_id
    group by doc.creator_id

)

, storage_used as (

    select
         user_id
        , iff(storage >= 250000000, '25mb or more', 'less than 25 mb') as chart_storage_buckets
    from user_storage_used
    where product = 'chart'

)

, used_microsoft_teams as (

    select
         user_id
        , microsoft_teams_first_used
    from c__user_integrations
    where microsoft_teams_first_used is not null

)

, presentation_mode as (

    select
         user_id
        , count(*) as number_of_presentation_modes
    from enteredPresentationMode
    group by user_id

)

, revision_history as (

    select
        user_id
        , count(*) as number_of_revision_restores
    from restoredDocumentFromRevisionHistory
    group by user_id

)

, final as (

    select
        users.*
        , usl_mindmap.user_id is not null as usl_mindmap
        , usl_timeline.user_id is not null as usl_timeline
        , usl_android.user_id is not null as usl_android
        , usl_ios.user_id is not null as usl_ios
        , usl_ios7.user_id is not null as usl_ios7
        , iff(dc.user_id is null, null, number_of_non_deleted_chart_documents) as number_of_non_deleted_chart_documents_over_3
        , iff(cg.user_id is null, null, number_of_comments_given) as count_comments_given_by_user
        , iff(cr.creator_id is null, null, number_of_comments_recieved) as count_comments_recieved_by_user
        , iff(su.user_id is null, null, chart_storage_buckets) as chart_storage_used
        , iff(umt.user_id is null, null, microsoft_teams_first_used) as first_used_microsoft_teams
        , iff(pm.user_id is null, null, number_of_presentation_modes) as count_presentation_mode
        , iff(rh.user_id is null, null, number_of_revision_restores) as count_revision_history
    from users
    left join used_shape_library as usl_mindmap
        on usl_mindmap.user_id = users.user_id
        and usl_mindmap.shape_library_name = 'mindmap'
    left join used_shape_library as usl_timeline
        on usl_timeline.user_id = users.user_id
        and usl_timeline.shape_library_name = 'timeline'
    left join used_shape_library as usl_android
        on usl_android.user_id = users.user_id
        and usl_android.shape_library_name = 'android'
    left join used_shape_library as usl_ios
        on usl_ios.user_id = users.user_id
        and usl_ios.shape_library_name = 'ios'
    left join used_shape_library as usl_ios7
        on usl_ios7.user_id = users.user_id
        and usl_ios7.shape_library_name = 'ios7'
    left join documents_created as dc on dc.user_id = users.user_id
    left join comments_given_by_user as cg on cg.user_id = users.user_id
    left join comments_recieved_by_user as cr on cr.creator_id = users.user_id
    left join storage_used as su on su.user_id = users.user_id
    left join used_microsoft_teams as umt on umt.user_id = users.user_id
    left join presentation_mode as pm on pm.user_id = users.user_id
    left join revision_history as rh on rh.user_id = users.user_id

)

select *
from final