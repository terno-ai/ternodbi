"""Report metrics and dimensions, as listed in TikTok's reporting docs.

* `BASIC`: (id, label, group index, flags) for every basic-report metric.
  Flags: "adv" = advertiser level only, "async" = async reports only.
* `AUDIENCE`, `PLAYABLE`, `BC`, `DSA`: metrics of those reports. A plain id
  reuses the basic metric's label; a list is (id, label, group).
* `DIMENSIONS`: dimensions per report type.
"""

import json

_PART1 = json.loads(r'''{"groups":["Attribute metrics","Core metrics","Interaction metrics/Videos metrics","Interaction metrics/Clicks metrics","Interaction metrics/Social metrics","Interaction metrics/LIVE metrics","Interaction metrics/Image metrics","Interaction metrics/Interactive add-ons metrics","Interaction metrics/Instant experience metrics","Conversions metrics/App metrics","Conversions metrics/Website metrics","Conversions metrics/TikTok metrics/Paid metrics","Conversions metrics/TikTok metrics/Native Growth metrics","Conversions metrics/Offline metrics","Conversions metrics/Messaging metrics","Conversions metrics/Shop metrics","Conversions metrics/Attribution metrics","SKAN metrics/Core metrics (SKAN)","SKAN metrics/App metrics (SKAN)","SKAN metrics/Attribution metrics (SKAN)"],"aud":[["advertiser_name","Account name","Attribute metrics"],["advertiser_id","Account ID","Attribute metrics"],"campaign_automation_type","campaign_name","campaign_id","objective_type","split_test","campaign_budget","campaign_dedicate_type","app_promotion_type","adgroup_name","adgroup_id","placement_type","promotion_type","opt_status","adgroup_download_url","profile_image","dpa_target_audience_type","budget","smart_target","pricing_category","billing_event","bid_strategy","bid","bid_secondary_goal","aeo_type","ad_name","ad_id_v2","ad_id","ad_text","call_to_action","ad_profile_image","ad_url","tt_app_id","tt_app_name","mobile_app_id",["device_brand_name","Device brand name","Attribute metrics"],["behavior_name","Behavior name","Attribute metrics"],["action_category","Action / behavior category","Attribute metrics"],["action_scene","Action scene","Attribute metrics"],["user_action","User action","Attribute metrics"],["action_period","Number of days for the action","Attribute metrics"],["rf_campaign_type","Campaign type","Attribute metrics"],["delivery_type","Delivery type","Attribute metrics"],["adgroup_display_name","Ad group display name","Attribute metrics"],["adgroup_schedule","Ad group schedule","Attribute metrics"],["optimization_preference","Optimization preference","Attribute metrics"],["image_info","Image information","Attribute metrics"],["ad_display_name","Ad display name","Attribute metrics"],["identity_type","Identity type","Attribute metrics"],"currency","is_aco","is_smart_creative","spend",["cost","Cost","Basic data metrics"],"cpc","cpm","impressions","gross_impressions","clicks","ctr","reach","cost_per_1000_reached","frequency","conversion","cost_per_conversion","conversion_rate","conversion_rate_v2",["real_time_conversion","Real-time conversions","Basic data metrics"],["real_time_cost_per_conversion","Real-time cost per conversion","Basic data metrics"],"real_time_conversion_rate",["real_time_conversion_rate_v2","Real-time conversion rate (CVR)","Basic data metrics"],"result","cost_per_result","result_rate","real_time_result","real_time_cost_per_result","real_time_result_rate","video_play_actions","video_watched_2s","video_watched_6s","engaged_view","engaged_view_15s","video_views_p25","video_views_p50","video_views_p75","video_views_p100","average_video_play","average_video_play_per_user","follows","likes","comments","shares","profile_visits","profile_visits_rate","clicks_on_music_disc","ix_page_duration_avg","ix_page_viewrate_avg",["vta_complete_payment","VTA complete payment","Attribution metrics"],["cost_per_vta_payments_completed","Cost per VTA complete payment","Attribution metrics"],["vta_complete_payment_value","Total VTA complete payment value","Attribution metrics"],["vta_complete_payment_roas","VTA complete payment ROAS","Attribution metrics"],["total_landing_page_view","Total Landing Page View","Page event metrics"],["cost_per_landing_page_view","Cost per Landing Page View","Page event metrics"],["landing_page_view_rate","Landing Page View Rate","Page event metrics"]],"play":[["playable_id","Playable ID","Attribute metrics"],["playable_name","Playable Name","Attribute metrics"],["playable_url","Playable URL","Attribute metrics"],["preview_url","Thumbnail","Attribute metrics"],["create_time","Create time","Attribute metrics"],["modify_time","Modify time","Attribute metrics"],["playable_status","Status","Attribute metrics"],["orientation","Display orientation","Attribute metrics"],["country","Delivery country or location","Attribute metrics"],["associated_adgroup_count","No. of Related Ad Groups","Attribute metrics"],"spend","cpm","impressions","clicks","ctr","conversion","cost_per_conversion","conversion_rate","conversion_rate_v2","currency",["app_install","App Install","In-App Event metrics (Unique)"],["cost_per_app_install","Cost per App Install","In-App Event metrics (Unique)"],["app_install_rate","App Install Rate","In-App Event metrics (Unique)"],["playable_page_view_count","Playable page views","Playable ads event metrics"],["playable_first_scene_view_count","Playable views of first scene","Playable ads event metrics"],["playable_second_scene_view_count","Playable views of second scene","Playable ads event metrics"],["playable_third_scene_view_count","Playable views of third scene","Playable ads event metrics"],["playable_cta_click","Playable download button clicks","Playable ads event metrics"]],"bc":[["advertiser_name","Ad Account name","Attribute metrics"],["timezone","Time zone","Attribute metrics"],["currency","Currency","Basic data metrics"],"spend","billed_cost",["cash_spend","Cost (Cash)","Basic data metrics"],["voucher_spend","Cost (Voucher)","Basic data metrics"],["cashback_coupon_spend","Cost (Cashback)","Basic data metrics"],["tax_spend","Cost (Tax)","Basic data metrics"],["cpc","CPC","Basic data metrics"],"cpm",["impressions","Impression","Basic data metrics"],["clicks","Clicks","Basic data metrics"],["ctr","CTR (Destination)","Basic data metrics"],"conversion",["cost_per_conversion","CPA","Basic data metrics"],["conversion_rate","CVR (Conversion rate)","Basic data metrics"],"reach",["real_time_conversion","Real-time conversions","Basic data metrics"],["real_time_cost_per_conversion","Real-time CPA","Basic data metrics"],["real_time_conversion_rate","Real-time CVR","Basic data metrics"],["account_type","Account type","Basic data metrics"],["skan_conversion","Conversions (SKAN)","Basic data metrics"],["skan_cost_per_conversion","CPA (SKAN)","Basic data metrics"],["skan_conversion_rate","CVR (SKAN)","Basic data metrics"],["registration","Unique registration","In-app events metrics"],["cost_per_registration","Cost per unique registration","In-app events metrics"],["registration_rate","Unique registration rate","In-app events metrics"],["purchase","Purchase","In-app events metrics"],["cost_per_purchase","Cost per purchase","In-app events metrics"],["purchase_rate","Unique purchase rate","In-app events metrics"],["value_per_total_purchase","Value per purchase","In-app events metrics"],["total_purchase_value","Total purchase value","In-app events metrics"],"app_install","cost_per_app_install",["app_install_rate","App install rate","In-app events metrics"],["real_time_app_install","Real-time app install","In-app events metrics"],["real_time_app_install_cost","Cost per real-time app install","In-app events metrics"],["real_time_app_install_rate","Real-time app install rate","In-app events metrics"],["skan_app_install","App install (SKAN)","In-app events (SKAN) metrics"],["skan_cost_per_app_install","Cost per app install (SKAN)","In-app events (SKAN) metrics"],["skan_registration","Unique registration (SKAN)","In-app events (SKAN) metrics"],["skan_cost_per_registration","Cost per unique registration (SKAN)","In-app events (SKAN) metrics"],["skan_registration_rate","Unique registration rate (SKAN)","In-app events (SKAN) metrics"],["skan_total_registration","Total registration (SKAN)","In-app events (SKAN) metrics"],["skan_cost_per_total_registration","Cost per registration (SKAN)","In-app events (SKAN) metrics"],["skan_purchase","Unique purchase (SKAN)","In-app events (SKAN) metrics"],["skan_cost_per_purchase","Cost per unique purchase (SKAN)","In-app events (SKAN) metrics"],["skan_purchase_rate","Unique purchase rate (SKAN)","In-app events (SKAN) metrics"],["skan_total_purchase","Total purchase (SKAN)","In-app events (SKAN) metrics"],["skan_cost_per_total_purchase","Cost per purchase (SKAN)","In-app events (SKAN) metrics"],["skan_total_purchase_value","Total purchase value (SKAN)","In-app events (SKAN) metrics"],"video_watched_2s","video_watched_6s","video_views_p100","video_views_p75","video_views_p50","video_views_p25",["contact_name","Contact name","Settings metrics"]],"dims":{"bdim":["advertiser_id","campaign_id","adgroup_id","ad_id_v2","ad_id","stat_time_day","stat_time_hour","country_code","ad_type","custom_event_type","event_source_id","page_id","component_name","room_id","post_id","image_id","video_material_id","minis_id","search_terms","search_keyword","match_type","skan_postback_sequence"],"adim":["advertiser_id","campaign_id","adgroup_id","ad_id_v2","ad_id","stat_time_day","stat_time_hour","gender","age","country_code","province_id","dma_id","ac","language","platform","interest_category","interest_category_tier2","interest_category_tier3","interest_category_tier4","behavior_id","placement","ad_type","device_brand_id","contextual_tag","hashtag","audience_tags","additional_interests","purchase_intention","bucket_id"],"pdim":["playable_id","country_code"],"ddim":["advertiser_id","campaign_id","adgroup_id","ad_id_v2","ad_id","stat_time_day","catalog_id_v2","product_id_v2","sku_id_v2","brand","content_category","catalog_id","product_set_id","product_id","sku_id"],"bcdim":["advertiser_id","stat_time_day","stat_time_week","stat_time_month","placement","country_code"]},"dsa":["product_name_v2","product_image_v2","catalog_name_v2","campaign_name","campaign_id","adgroup_name","adgroup_id","ad_id_v2","ad_name","conversion","product_impressions","product_clicks","product_cta","purchase","total_purchase","value_per_total_purchase","total_purchase_value","app_event_add_to_cart","total_app_event_add_to_cart","value_per_total_app_event_add_to_cart","total_app_event_add_to_cart_value","checkout","total_checkout","value_per_checkout","total_checkout_value","view_content","total_view_content","value_per_total_view_content","total_view_content_value","complete_payment","value_per_complete_payment","total_complete_payment_rate","total_pageview","avg_value_per_pageview","total_value_per_pageview","page_content_view_events","value_per_page_content_view_event","total_page_view_content_events_value","web_event_add_to_cart","value_per_web_event_add_to_cart","total_web_event_add_to_cart_value","initiate_checkout","value_per_initiate_checkout","total_initiate_checkout_value","offline_shopping_events","value_per_offline_shopping_event","offline_shopping_events_value","vta_conversion","vta_complete_payment","vta_complete_payment_value","cta_conversion","cta_complete_payment","cta_complete_payment_value","catalog_name","product_set_name","product_name","placement_type","ad_text","product_image","tt_app_id","tt_app_name","mobile_app_id","promotion_type","dpa_target_audience_type","impressions","clicks","spend","cost_per_conversion","conversion_rate","conversion_rate_v2","ctr","cpc","cpm","real_time_result","real_time_cost_per_result","real_time_result_rate","result","cost_per_result","result_rate","currency","real_time_app_install","real_time_app_install_cost","app_install","cost_per_app_install","registration","cost_per_registration","registration_rate","total_registration","cost_per_total_registration","cost_per_purchase","purchase_rate","cost_per_total_purchase","total_active_pay_roas","cost_per_app_event_add_to_cart","app_event_add_to_cart_rate","cost_per_total_app_event_add_to_cart","cost_per_checkout","checkout_rate","cost_per_total_checkout","cost_per_view_content","view_content_rate","cost_per_total_view_content","next_day_open","cost_per_next_day_open","next_day_open_rate","total_next_day_open","cost_per_total_next_day_open","add_payment_info","cost_per_add_payment_info","add_payment_info_rate","total_add_payment_info","cost_total_add_payment_info","add_to_wishlist","cost_per_add_to_wishlist","add_to_wishlist_rate","total_add_to_wishlist","cost_per_total_add_to_wishlist","value_per_total_add_to_wishlist","total_add_to_wishlist_value","launch_app","cost_per_launch_app","launch_app_rate","total_launch_app","cost_per_total_launch_app","complete_tutorial","cost_per_complete_tutorial","complete_tutorial_rate","total_complete_tutorial","cost_per_total_complete_tutorial","value_per_total_complete_tutorial","total_complete_tutorial_value","create_group","cost_per_create_group","create_group_rate","total_create_group","cost_per_total_create_group","value_per_total_create_group","total_create_group_value","join_group","cost_per_join_group","join_group_rate","total_join_group","cost_per_total_join_group","value_per_total_join_group","total_join_group_value","create_gamerole","cost_per_create_gamerole","create_gamerole_rate","total_create_gamerole","cost_per_total_create_gamerole","value_per_total_create_gamerole","total_create_gamerole_value","spend_credits","cost_per_spend_credits","spend_credits_rate","total_spend_credits","cost_per_total_spend_credits","value_per_total_spend_credits","total_spend_credits_value","achieve_level","cost_per_achieve_level","achieve_level_rate","total_achieve_level","cost_per_total_achieve_level","value_per_total_achieve_level","total_achieve_level_value","unlock_achievement","cost_per_unlock_achievement","unlock_achievement_rate","total_unlock_achievement","cost_per_total_unlock_achievement","value_per_total_unlock_achievement","total_unlock_achievement_value","sales_lead","cost_per_sales_lead","sales_lead_rate","total_sales_lead","cost_per_total_sales_lead","value_per_total_sales_lead","total_sales_lead_value","in_app_ad_click","cost_per_in_app_ad_click","in_app_ad_click_rate","total_in_app_ad_click","cost_per_total_in_app_ad_click","value_per_total_in_app_ad_click","total_in_app_ad_click_value","in_app_ad_impr","cost_per_in_app_ad_impr","in_app_ad_impr_rate","total_in_app_ad_impr","cost_per_total_in_app_ad_impr","value_per_total_in_app_ad_impr","total_in_app_ad_impr_value","loan_apply","cost_per_loan_apply","loan_apply_rate","total_loan_apply","cost_per_total_loan_apply","loan_credit","cost_per_loan_credit","loan_credit_rate","total_loan_credit","cost_per_total_loan_credit","loan_disbursement","cost_per_loan_disbursement","loan_disbursement_rate","total_loan_disbursement","cost_per_total_loan_disbursement","login","cost_per_login","login_rate","total_login","cost_per_total_login","ratings","cost_per_ratings","ratings_rate","total_ratings","cost_per_total_ratings","value_per_total_ratings","total_ratings_value","search","cost_per_search","search_rate","total_search","cost_per_total_search","start_trial","cost_per_start_trial","start_trial_rate","total_start_trial","cost_per_total_start_trial","subscribe","cost_per_subscribe","subscribe_rate","total_subscribe","cost_per_total_subscribe","value_per_total_subscribe","total_subscribe_value","complete_payment_roas","cost_per_complete_payment","complete_payment_rate","value_per_button_click","total_button_click_value","user_registration","cost_per_user_registration","user_registration_rate","value_per_user_registration","total_user_registration_value","cost_per_web_event_add_to_cart","web_event_add_to_cart_rate","on_web_order","cost_per_on_web_order","on_web_order_rate","value_per_on_web_order","total_on_web_order_value","cost_per_initiate_checkout","initiate_checkout_rate","add_billing","cost_per_add_billing","add_billing_rate","value_per_add_billing","total_add_billing_value","page_event_search","cost_per_page_event_search","page_event_search_rate","value_per_page_event_search","total_page_event_search_value","form","cost_per_form","form_rate","value_per_form","total_form_value","download_start","cost_per_download_start","download_start_rate","value_per_download_start","total_download_start_value","on_web_add_to_wishlist","cost_per_on_web_add_to_wishlist","on_web_add_to_wishlist_per_click","value_per_on_web_add_to_wishlist","total_on_web_add_to_wishlist_value","on_web_subscribe","cost_per_on_web_subscribe","on_web_subscribe_per_click","value_per_on_web_subscribe","total_on_web_subscribe_value"]}''')

GROUPS = tuple(_PART1["groups"])
AUDIENCE = _PART1["aud"]
PLAYABLE = _PART1["play"]
BC = _PART1["bc"]
DSA = _PART1["dsa"]
DIMENSIONS = {
    "BASIC": tuple(_PART1["dims"]["bdim"]),
    "AUDIENCE": tuple(_PART1["dims"]["adim"]),
    "PLAYABLE_MATERIAL": tuple(_PART1["dims"]["pdim"]),
    "CATALOG": tuple(_PART1["dims"]["ddim"]),
    "BC": tuple(_PART1["dims"]["bcdim"]),
}

# One metric per line: "id|label|group index|flags".
_BASIC_1 = r'''advertiser_name|Advertiser account name|0|
advertiser_id|Advertiser account ID|0|
campaign_automation_type|Campaign automation type. Enum values: MANUAL, SMART_PLUS, UPGRADED_SMART_PLUS, UPGRADED_SMART_PLUS_CREATIVE.|0|
campaign_name|Campaign name|0|
campaign_id|Campaign ID|0|
objective_type|Advertising objective|0|
split_test|Split test status|0|
campaign_budget|Campaign budget|0|
campaign_dedicate_type|Campaign type|0|
app_promotion_type|App promotion type|0|
adgroup_name|Ad group name|0|
adgroup_id|Ad group ID|0|
placement_type|Placement type|0|
promotion_type|Promotion type|0|
opt_status|Automated creative optimization|0|
adgroup_download_url|Download URL/Website URL|0|
profile_image|Profile image|0|
dpa_target_audience_type|Target audience type for DPA|0|
budget|Ad group budget|0|
smart_target|Optimization goal|0|
pricing_category|Billing Event|0|
billing_event|Billing Event|0|
bid_strategy|Bid strategy|0|
bid|Bid|0|
bid_secondary_goal|Bid for secondary goal|0|
aeo_type|App Event Optimization Type|0|
ad_name|Ad name|0|
ad_id_v2|Ad ID (Manual, Smart+ and Upgraded Smart+ ads). Cannot be used with the dimension ad_id, the filter ad_ids, or the attribute metric ad_id.|0|
ad_id|Ad ID|0|
ad_text|Ad title|0|
call_to_action|Call to action|0|
ad_profile_image|Profile image (Ad level)|0|
ad_url|URL (Ad level)|0|
tt_app_id|TikTok App ID|0|
tt_app_name|TikTok App Name|0|
mobile_app_id|Mobile App ID|0|
image_mode|Format|0|
currency|currency|0|
is_aco|Whether the ad is an automated ad or a Smart Creative ad.|0|
is_smart_creative|Whether the ad is a Smart Creative ad.|0|
minis_name|Minis name. Only with the dimension minis_id.|0|
optimization_event|Optimization event. Enum values: IAA, IAP, UNSET.|0|
spend|Cost|1|
billed_cost|Net cost|1|
cash_spend|Cost Charged by Cash|1|adv
voucher_spend|Cost Charged by Voucher|1|adv
cpc|CPC (destination)|1|
cpm|CPM|1|
impressions|Impressions|1|
gross_impressions|Gross Impressions (Includes Invalid Impressions)|1|
clicks|Clicks (destination)|1|
ctr|CTR (destination)|1|
reach|Reach|1|
cost_per_1000_reached|Cost per 1,000 people reached|1|
frequency|Frequency|1|
conversion|Conversions|1|
cost_per_conversion|Cost per conversion|1|
conversion_rate|Conversion rate (CVR, clicks)|1|
conversion_rate_v2|Conversion rate (CVR)|1|
real_time_conversion|Conversions by conversion time|1|
real_time_cost_per_conversion|Cost per conversion by conversion time|1|
real_time_conversion_rate|Real-time conversion rate (CVR, clicks)|1|
real_time_conversion_rate_v2|Conversion rate (CVR) by conversion time|1|
result|Results|1|
cost_per_result|Cost per result|1|
result_rate|Result rate|1|
real_time_result|Real-time results|1|
real_time_cost_per_result|Real-time cost per result|1|
real_time_result_rate|Real-time result rate|1|
secondary_goal_result|Deep funnel result|1|
cost_per_secondary_goal_result|Cost per deep funnel result|1|
secondary_goal_result_rate|Deep funnel result rate|1|
video_play_actions|Video views|2|
video_watched_2s|2-second video views|2|
video_watched_6s|6-second video views|2|
video_views_p25|Video views at 25%|2|
video_views_p50|Video views at 50%|2|
video_views_p75|Video views at 75%|2|
video_views_p100|Video views at 100%|2|
average_video_play|Average play time per video view|2|
average_video_play_per_user|Average play time per user|2|
engaged_view|6-second focused views|2|
paid_engaged_view|6-second focused views (paid views)|2|
paid_engagement_engaged_view|6-second focused views (paid interactions)|2|
engaged_view_15s|15-second focused views|2|
paid_engaged_view_15s|15-second focused views (paid views)|2|
paid_engagement_engaged_view_15s|15-second focused views (paid interactions)|2|
engagements|Clicks (all)|3|
engagement_rate|CTR (all)|3|
clicks_on_music_disc|Sound clicks|3|
duet_clicks|Duet button clicks|3|
stitch_clicks|Stitch button clicks|3|
sound_usage_clicks|Use sound button clicks|3|
anchor_clicks|Anchor clicks|3|
anchor_click_rate|Anchor click rate|3|
clicks_on_hashtag_challenge|Hashtag clicks|3|
follows|Paid follows|4|
likes|Paid likes|4|
comments|Paid comments|4|
shares|Paid shares|4|
profile_visits|Paid profile visits|4|
profile_visits_rate|Paid profile visit rate|4|
tt_playlist_visit|Playlist page visit|4|
tt_playlist_visit_rate|Playlist page visit rate|4|
cost_per_consideration|Cost per consideration|4|
new_consideration_rate|New consideration rate|4|
new_consideration_size|New consideration size|4|
live_views|LIVE views|5|
live_unique_views|LIVE unique views|5|
live_effective_views|10-second LIVE views|5|
live_product_clicks|LIVE Product Clicks|5|
single_image_impressions|Single image impression|6|
single_image_impression_rate|Single image showrate|6|
single_image_ctr|Single image CTR|6|
interactive_add_on_impressions|Interactive Add-on impressions|7|
interactive_add_on_destination_clicks|Interactive Add-on destination clicks|7|
interactive_add_on_activity_clicks|Interactive Add-on activity clicks|7|
interactive_add_on_option_a_clicks|Interactive Add-on option A clicks|7|
interactive_add_on_option_b_clicks|Interactive Add-on option B clicks|7|
countdown_sticker_recall_clicks|Countdown sticker recall clicks|7|
ix_page_duration_avg|Instant experience average view time|8|
ix_page_viewrate_avg|Instant experience average view percentage|8|
ix_video_views|Instant experience video views|8|
ix_video_views_p25|Instant experience video views at 25%|8|
ix_video_views_p50|Instant experience video views at 50%|8|
ix_video_views_p75|Instant experience video views at 75%|8|
ix_video_views_p100|Instant experience video views at 100%|8|
ix_average_video_play|Average Instant experience video view time|8|
real_time_app_install|App installs by conversion time|9|
real_time_app_install_cost|Cost per app install by conversion time|9|
app_install|App install|9|
cost_per_app_install|Cost per app install|9|
registration|Unique registrations (app)|9|
cost_per_registration|Unique cost per registration (app)|9|
registration_rate|Unique registration rate (app)|9|
total_registration|Registrations (app)|9|
cost_per_total_registration|Cost per registration (app)|9|
purchase|Unique purchases (app)|9|
cost_per_purchase|Unique cost per purchase (app)|9|
purchase_rate|Unique purchase rate (app)|9|
total_purchase|Purchases (app)|9|
cost_per_total_purchase|Cost per purchase (app)|9|
value_per_total_purchase|Value per purchase (app)|9|
total_purchase_value|Purchase value (app)|9|
total_active_pay_roas|Purchase ROAS (app)|9|
total_purchase_roas_day0|Day 0 purchase ROAS|9|
total_purchase_roas_day2|Day 2 purchase ROAS|9|
total_purchase_roas_day6|Day 6 purchase ROAS|9|
total_purchase_value_day0|Day 0 purchase value|9|
total_purchase_value_day2|Day 2 purchase value|9|
total_purchase_value_day6|Day 6 purchase value|9|
app_event_add_to_cart|Unique adds to cart (app)|9|
cost_per_app_event_add_to_cart|Unique cost per add to cart (app)|9|
app_event_add_to_cart_rate|Unique add to cart rate (app)|9|
total_app_event_add_to_cart|Adds to cart (app)|9|
cost_per_total_app_event_add_to_cart|Cost per add to cart (app)|9|
value_per_total_app_event_add_to_cart|Value per add to cart (app)|9|
total_app_event_add_to_cart_value|Add to cart value (app)|9|
checkout|Unique checkouts (app)|9|
cost_per_checkout|Unique cost per checkout (app)|9|
checkout_rate|Unique checkout rate (app)|9|
total_checkout|Checkouts (app)|9|
cost_per_total_checkout|Cost per checkout (app)|9|
value_per_checkout|Value per checkout (app)|9|
total_checkout_value|Checkout value (app)|9|
view_content|Unique content views (app)|9|
cost_per_view_content|Unique cost per content view (app)|9|
view_content_rate|Unique content view rate (app)|9|
total_view_content|Content views (app)|9|
cost_per_total_view_content|Cost per content view (app)|9|
value_per_total_view_content|Value per content view (app)|9|
total_view_content_value|Content view value (app)|9|
next_day_open|Unique Day 1 retentions|9|
cost_per_next_day_open|Cost per unique Day 1 retention|9|
next_day_open_rate|Day 1 retention rate (%)|9|
total_next_day_open|Day 1 retentions|9|
cost_per_total_next_day_open|Cost per Day 1 retention|9|
day7_retention|Unique Day 7 retentions|9|
cost_per_day7_retention|Cost per unique Day 7 retention|9|
day7_retention_rate|Day 7 retention rate (%)|9|
total_day7_retention|Day 7 retentions|9|
cost_per_total_day7_retention|Cost per Day 7 retention|9|
value_per_total_day7_retention|Value per Day 7 retention|9|
total_day7_retention_value|Day 7 retention value|9|
add_payment_info|Unique payment info adds (app)|9|
cost_per_add_payment_info|Unique cost per payment info add (app)|9|
add_payment_info_rate|Unique payment info add rate (app)|9|
total_add_payment_info|Payment info adds (app)|9|
cost_total_add_payment_info|Cost per payment info add (app)|9|
add_to_wishlist|Unique adds to wishlist (app)|9|
cost_per_add_to_wishlist|Unique cost per add to wishlist (app)|9|
add_to_wishlist_rate|Unique add to wishlist rate (app)|9|
total_add_to_wishlist|Adds to wishlist (app)|9|
cost_per_total_add_to_wishlist|Cost per add to wishlist (app)|9|
value_per_total_add_to_wishlist|Value per add to wishlist (app)|9|
total_add_to_wishlist_value|Add to wishlist value (app)|9|
launch_app|Unique app launches (app)|9|
cost_per_launch_app|Unique cost per app launch (app)|9|
launch_app_rate|Unique app launch rate (app)|9|
total_launch_app|App launches (app)|9|
cost_per_total_launch_app|Cost per app launch (app)|9|
complete_tutorial|Unique tutorials completed (app)|9|
cost_per_complete_tutorial|Unique cost per complete tutorial (app)|9|
complete_tutorial_rate|Unique tutorial completion rate (app)|9|
total_complete_tutorial|Tutorials completed (app)|9|
cost_per_total_complete_tutorial|Cost per complete tutorial (app)|9|
value_per_total_complete_tutorial|Value per complete tutorial (app)|9|
total_complete_tutorial_value|Tutorial completion value (app)|9|
create_group|Unique groups created (app)|9|
cost_per_create_group|Unique cost per group created (app)|9|
create_group_rate|Unique group creation rate (app)|9|
total_create_group|Groups created (app)|9|
cost_per_total_create_group|Cost per group created (app)|9|
value_per_total_create_group|Value per group created (app)|9|
total_create_group_value|Group creation value (app)|9|
join_group|Unique group joins (app)|9|
cost_per_join_group|Unique cost per group join (app)|9|
join_group_rate|Unique group joining rate (app)|9|
total_join_group|Group joins (app)|9|
cost_per_total_join_group|Cost per group join (app)|9|
value_per_total_join_group|Value per group join (app)|9|
total_join_group_value|Group joining value (app)|9|
create_gamerole|Unique roles created (app)|9|
cost_per_create_gamerole|Unique cost per role created (app)|9|
create_gamerole_rate|Unique role creation rate (app)|9|
total_create_gamerole|Roles created (app)|9|
cost_per_total_create_gamerole|Cost per role created (app)|9|
value_per_total_create_gamerole|Value per role created (app)|9|
total_create_gamerole_value|Role creation value (app)|9|
spend_credits|Unique credit spends (app)|9|
cost_per_spend_credits|Unique cost per credit spend (app)|9|
spend_credits_rate|Unique credit spend rate (app)|9|
total_spend_credits|Credit spends (app)|9|
cost_per_total_spend_credits|Cost per credit spend (app)|9|
value_per_total_spend_credits|Value per credit spend (app)|9|
total_spend_credits_value|Credit spend value (app)|9|
achieve_level|Unique levels achieved (app)|9|
cost_per_achieve_level|Unique cost per unique level achieved (app)|9|
achieve_level_rate|Unique level achievement rate (app)|9|
total_achieve_level|Levels achieved (app)|9|
cost_per_total_achieve_level|Cost per level achieved (app)|9|
value_per_total_achieve_level|Value per level achieved (app)|9|
total_achieve_level_value|Level achievement value (app)|9|
unlock_achievement|Unique achievements unlocked (app)|9|
cost_per_unlock_achievement|Unique cost per achievement unlocked (app)|9|
unlock_achievement_rate|Unique unlocked achievement rate (app)|9|
total_unlock_achievement|Achievements unlocked (app)|9|
cost_per_total_unlock_achievement|Cost per achievement unlocked (app)|9|
value_per_total_unlock_achievement|Value per achievement unlocked (app)|9|
total_unlock_achievement_value|Unlocked achievement value (app)|9|
sales_lead|Unique leads generated (app)|9|
cost_per_sales_lead|Unique cost per lead generated (app)|9|
sales_lead_rate|Unique lead generation rate (app)|9|
total_sales_lead|Leads generated (app)|9|
cost_per_total_sales_lead|Cost per lead generated (app)|9|
value_per_total_sales_lead|Value per lead generated (app)|9|
total_sales_lead_value|Lead generation value (app)|9|
in_app_ad_click|Unique in-app ad clicks (app)|9|
cost_per_in_app_ad_click|Unique cost per in-app ad click (app)|9|
in_app_ad_click_rate|In-app ad click rate (app)|9|
total_in_app_ad_click|In-app ad clicks (app)|9|
cost_per_total_in_app_ad_click|Cost per in-app ad click (app)|9|
value_per_total_in_app_ad_click|Value per in-app ad click (app)|9|
total_in_app_ad_click_value|In-app ad click value (app)|9|
in_app_ad_impr|Unique in-app ad impressions (app)|9|
cost_per_in_app_ad_impr|Unique cost per in-app ad impression (app)|9|
in_app_ad_impr_rate|In-app ad impression rate (app)|9|
total_in_app_ad_impr|In-app ad impressions (app)|9|
cost_per_total_in_app_ad_impr|Cost per in-app ad impression (app)|9|
value_per_total_in_app_ad_impr|Value per in-app ad impression (app)|9|
total_in_app_ad_impr_value|In-app ad impression value (app)|9|
loan_apply|Unique loan applications (app)|9|
cost_per_loan_apply|Unique cost per loan application (app)|9|
loan_apply_rate|Unique loan application rate (app)|9|
total_loan_apply|Loan applications (app)|9|
cost_per_total_loan_apply|Cost per loan application (app)|9|
loan_credit|Unique loan approvals (app)|9|
cost_per_loan_credit|Unique cost per loan approval (app)|9|
loan_credit_rate|Unique loan approval rate (app)|9|
total_loan_credit|Loan approvals (app)|9|
cost_per_total_loan_credit|Cost per loan approval (app)|9|
loan_disbursement|Unique loan disbursals (app)|9|
cost_per_loan_disbursement|Unique cost per loan disbursal (app)|9|
loan_disbursement_rate|Unique loan disbursal rate (app)|9|
total_loan_disbursement|Loan disbursals (app)|9|
cost_per_total_loan_disbursement|Cost per loan disbursal (app)|9|
login|Unique logins (app)|9|
cost_per_login|Unique cost per login (app)|9|
login_rate|Unique login rate (app)|9|
total_login|Logins (app)|9|
cost_per_total_login|Cost per login (app)|9|
ratings|Unique ratings (app)|9|
cost_per_ratings|Unique cost per rating (app)|9|
ratings_rate|Unique rate rate (%)|9|
total_ratings|Ratings (app)|9|
cost_per_total_ratings|Cost per rating (app)|9|
value_per_total_ratings|Value per rating (app)|9|
total_ratings_value|Rating value (app)|9|
search|Unique searches (app)|9|
cost_per_search|Unique cost per search (app)|9|
search_rate|Unique search rate (app)|9|
total_search|Searches (app)|9|
cost_per_total_search|Cost per search (app)|9|
start_trial|Unique trial started (app)|9|
cost_per_start_trial|Unique cost per trial started (app)|9|
start_trial_rate|Unique trial start rate (app)|9|
total_start_trial|Trials started (app)|9|
cost_per_total_start_trial|Cost per trial started (app)|9|
subscribe|Unique subscriptions (app)|9|
cost_per_subscribe|Unique cost per subscription (app)|9|
subscribe_rate|Unique subscription rate (app)|9|
total_subscribe|Subscriptions (app)|9|
cost_per_total_subscribe|Cost per subscription (app)|9|
value_per_total_subscribe|Value per subscription (app)|9|
total_subscribe_value|Subscription value (app)|9|
unique_custom_app_events|Unique custom events (app)|9|
cost_per_unique_custom_app_event|Cost per unique custom event (app)|9|
custom_app_event_rate|Custom event rate (app) (%)|9|
custom_app_events|Custom events (app)|9|
cost_per_custom_app_event|Cost per custom event (app)|9|
value_per_custom_app_event|Value per custom event (app)|9|
custom_app_events_value|Custom event value (app)|9|
unique_ad_impression_events|Unique ad impressions|9|
cost_per_unique_ad_impression_event|Cost per unique ads impression|9|
customized_ad_impression_event_rate|Unique ad impression rate (%)|9|
ads_impression_events|Ads impressions|9|
cost_pre_ad_impression_event|Cost per ads impression|9|
value_per_ad_impression_event|Value per ads impression|9|
total_ad_impression_events_value|Ads impression value|9|
total_ad_impression_roas|Ads impression ROAS|9|
ad_impression_ad_revenue_roas_day0|Day 0 ad revenue ROAS|9|
ad_impression_ad_revenue_roas_day2|Day 2 ad revenue ROAS|9|
ad_impression_ad_revenue_roas_day6|Day 6 ad revenue ROAS|9|
ad_impression_ad_revenue_day0|Day 0 ad revenue|9|
ad_impression_ad_revenue_day2|Day 2 ad revenue|9|
ad_impression_ad_revenue_day6|Day 6 ad revenue|9|
complete_payment_roas|Purchase ROAS (website)|10|
complete_payment|Purchases (website)|10|
cost_per_complete_payment|Cost per purchase (website)|10|
complete_payment_rate|Unique purchase rate (website) (%)|10|
value_per_complete_payment|Value per purchase (website)|10|
total_complete_payment_rate|Purchase value (website)|10|
total_landing_page_view|Landing page views (website)|10|
cost_per_landing_page_view|Cost per landing page view (website)|10|
landing_page_view_rate|Landing page view rate (website)|10|
total_pageview|Page views (website)|10|
cost_per_pageview|Cost per page view (website)|10|
pageview_rate|Page view rate (website) (%)|10|
avg_value_per_pageview|Value per page view (website)|10|
total_value_per_pageview|Total page view value (website)|10|
page_browse_view|Page Browse|10|
cost_per_page_browse_view|Cost per Page Browse|10|
page_browse_view_rate|Page Browse Rate (%)|10|
total_page_browse_view_value|Value per Page Browse|10|
value_per_page_browse_view|Total Page Browse Value|10|
button_click|Button clicks (website)|10|
cost_per_button_click|Cost per button click (website)|10|
button_click_rate|Unique button click rate (website)|10|
value_per_button_click|Value per button click (website)|10|
total_button_click_value|Button click value (website)|10|
online_consult|Contacts (website)|10|
cost_per_online_consult|Cost per contact (website)|10|
online_consult_rate|Unique contact rate (website)|10|
value_per_online_consult|Value per contact (website)|10|
total_online_consult_value|Contact value (website)|10|
user_registration|Registrations (website)|10|
cost_per_user_registration|Cost per registration (website)|10|
user_registration_rate|Unique registration rate (website) (%)|10|
value_per_user_registration|Value per registration (website)|10|
total_user_registration_value|Registration value (website)|10|
page_content_view_events|Content views (website)|10|
cost_per_page_content_view_event|Cost per content view (website)|10|
page_content_view_event_rate|Unique content view rate (website) (%)|10|
value_per_page_content_view_event|Value per content view (website)|10|
total_page_view_content_events_value|Content view value (website)|10|
product_details_page_browse|Product Details Page Browse|10|
cost_per_product_details_page_browse|Product Details Page Browse Cost|10|
product_details_page_browse_rate|Product Details Page Browse Rate (%)|10|
value_per_product_details_page_browse|Value per Product Details Page Browse|10|
total_product_details_page_browse_value|Total Product Details Page Browse Value|10|
web_event_add_to_cart|Adds to cart (website)|10|
cost_per_web_event_add_to_cart|Cost per add to cart (website)|10|
web_event_add_to_cart_rate|Unique add to cart rate (website) (%)|10|
value_per_web_event_add_to_cart|Value per add to cart (website)|10|
total_web_event_add_to_cart_value|Add to cart value (website)|10|
on_web_order|Orders placed (website)|10|
cost_per_on_web_order|Cost per order placed (website)|10|
on_web_order_rate|Unique orders placed rate (website) (%)|10|
value_per_on_web_order|Value per order placed (website)|10|
total_on_web_order_value|Orders placed value (website)|10|
initiate_checkout|Checkouts initiated (website)|10|
cost_per_initiate_checkout|Cost per checkout initiated (website)|10|
initiate_checkout_rate|Unique checkout initiation rate (website) (%)|10|
value_per_initiate_checkout|Value per checkout initiated (website)|10|
total_initiate_checkout_value|Checkout initiation value (website)|10|
add_billing|Payment info added (website)|10|
cost_per_add_billing|Cost per payment info added (website)|10|
add_billing_rate|Unique payment info added rate (%) (website)|10|
value_per_add_billing|Value per payment info added (website)|10|
total_add_billing_value|Add payment info value (website)|10|
page_event_search|Searches (website)|10|
cost_per_page_event_search|Cost per search (website)|10|
page_event_search_rate|Unique search rate (website) (%)|10|
value_per_page_event_search|Value per search (website)|10|
total_page_event_search_value|Search value (website)|10|
form|Form submissions (website)|10|
cost_per_form|Cost per form submitted (website)|10|
form_rate|Unique form submission rate (website) (%)|10|
value_per_form|Value per form submitted (website)|10|
total_form_value|Form submission value (website)|10|
download_start|Downloads (website)|10|
cost_per_download_start|Cost per download (website)|10|
download_start_rate|Unique download rate (website) (%)|10|
value_per_download_start|Value per download (website)|10|
total_download_start_value|Download value (website)|10|
on_web_add_to_wishlist|Adds to wishlist (website)|10|
cost_per_on_web_add_to_wishlist|Cost per add to wishlist (website)|10|
on_web_add_to_wishlist_per_click|Unique add to wishlist rate (website) (%)|10|
value_per_on_web_add_to_wishlist|Value per add to wishlist (website)|10|
total_on_web_add_to_wishlist_value|Add to wishlist value (website)|10|
on_web_subscribe|Subscriptions (website)|10|
cost_per_on_web_subscribe|Cost per subscription (website)|10|
on_web_subscribe_per_click|Unique subscription rate (website) (%)|10|
value_per_on_web_subscribe|Value per subscription (website)|10|
total_on_web_subscribe_value|Subscription value (website)|10|
website_total_find_location|Total find location|10|
website_cost_per_find_location|Cost per find location|10|
website_find_location_rate|Find location rate|10|
website_value_per_find_location|Value per find location|10|
website_total_find_location_value|Total find location value|10|
website_total_schedule|Total schedule|10|
website_cost_per_schedule|Cost per schedule|10|
website_schedule_rate|Schedule rate|10|
website_value_per_schedule|Value per schedule|10|
website_total_schedule_value|Total schedule value|10|
website_total_customize_product|Total customize product|10|
website_cost_per_customize_product|Cost per customize product|10|
website_customize_product_rate|Customize product rate|10|
website_value_per_customize_product|Value per customize product|10|
website_total_customize_product_value|Total customize product value|10|
custom_page_events|Custom events (website)|10|
cost_per_custom_page_event|Cost per custom event (website)|10|
custom_page_event_rate|Custom event rate (website) (%)|10|
value_per_custom_page_event|Value per custom event (website)|10|
custom_page_events_value|Custom event value (website)|10|
onsite_purchases_roas|Purchases ROAS (TikTok)|11|
onsite_total_purchase|Purchases (TikTok)|11|
onsite_cost_per_purchase|Cost per purchase (TikTok)|11|
onsite_purchase_rate|Purchase rate (TikTok) (%)|11|
onsite_value_per_purchase|Value per purchase (TikTok)|11|
onsite_total_purchase_value|Purchase value (TikTok)|11|
onsite_unique_purchase|Unique purchase|11|
onsite_cost_per_unique_purchase|Cost per unique purchase|11|
onsite_total_purchase_value_calendar_day0|Calendar day 0 purchase value|11|
onsite_total_purchase_value_calendar_day6|Calendar day 6 purchase value|11|
onsite_total_purchase_value_calendar_day13|Calendar day 13 purchase value|11|
onsite_total_purchase_roas_calendar_day0|Calendar Day 0 purchase ROAS|11|
onsite_total_purchase_roas_calendar_day6|Calendar day 6 purchase ROAS|11|
onsite_total_purchase_roas_calendar_day13|Calendar day 13 purchase ROAS|11|
onsite_total_purchase_value_day0|Day 0 purchase value|11|
onsite_total_purchase_value_day1|Day 1 purchase value|11|
onsite_total_purchase_value_day2|Day 2 purchase value|11|
onsite_total_purchase_value_day3|Day 3 purchase value|11|
onsite_total_purchase_value_day4|Day 4 purchase value|11|
onsite_total_purchase_value_day5|Day 5 purchase value|11|
onsite_total_purchase_value_day6|Day 6 purchase value|11|
onsite_total_purchase_value_day13|Day 13 purchase value|11|
onsite_total_purchase_value_day20|Day 20 purchase value|11|
onsite_total_purchase_value_day27|Day 27 purchase value|11|
onsite_total_purchase_value_day29|Day 29 purchase value|11|
onsite_total_purchase_roas_day0|Day 0 purchase ROAS|11|
onsite_total_purchase_roas_day1|Day 1 purchase ROAS|11|
onsite_total_purchase_roas_day2|Day 2 purchase ROAS|11|
onsite_total_purchase_roas_day3|Day 3 purchase ROAS|11|
onsite_total_purchase_roas_day4|Day 4 purchase ROAS|11|
onsite_total_purchase_roas_day5|Day 5 purchase ROAS|11|
onsite_total_purchase_roas_day6|Day 6 purchase ROAS|11|
onsite_total_purchase_roas_day13|Day 13 purchase ROAS|11|
onsite_total_purchase_roas_day20|Day 20 purchase ROAS|11|
onsite_total_purchase_roas_day27|Day 27 purchase ROAS|11|
onsite_total_purchase_roas_day29|Day 29 purchase ROAS|11|
onsite_total_checkout_initiation|Checkouts initiated (TikTok)|11|
onsite_cost_per_checkout_initiation|Cost per checkout initiation (TikTok)|11|
onsite_checkout_initiation_rate|Checkout initiation rate (TikTok) (%)|11|
onsite_value_per_checkout_initiation|Value per checkout initiation (TikTok)|11|
onsite_total_checkout_initiation_value|Checkout initiation value (TikTok)|11|
onsite_unique_checkout_initiation|Unique checkout|11|
onsite_cost_per_unique_checkout_initiation|Cost per unique checkout|11|
onsite_total_add_to_cart|Add to cart (TikTok)|11|
onsite_cost_per_add_to_cart|Cost per add to cart (TikTok)|11|
onsite_add_to_cart_rate|Add to cart rate (TikTok) (%)|11|
onsite_value_per_add_to_cart|Value per add to cart (TikTok)|11|
onsite_total_add_to_cart_value|Add to cart value (TikTok)|11|
onsite_unique_add_to_cart|Unique add to cart events|11|
onsite_cost_per_unique_add_to_cart|Cost per unique add to cart action|11|
onsite_total_product_details_page_view|Product details page view (TikTok)|11|
onsite_cost_per_product_details_page_view|Cost per product details page view (TikTok)|11|
onsite_product_details_page_view_rate|Product details page view rate (TikTok) (%)|11|
onsite_value_per_product_details_page_view|Value per product details page view (TikTok)|11|
onsite_total_product_details_page_view_value|Product details page view value (TikTok)|11|
onsite_unique_first_launch_conversion_time|Active by conversion time|11|
onsite_cost_per_unique_first_launch_conversion_time|Cost per active by conversion time|11|
onsite_unique_first_launch|Active by impression time|11|
onsite_cost_per_unique_first_launch|Cost per active by impression time|11|
onsite_unique_non_first_launch|Unique launch|11|
onsite_cost_per_unique_non_first_launch|Cost per unique launch|11|
onsite_launch_rate|Launch rate (%)|11|
onsite_total_non_first_launch|Total launch|11|
onsite_cost_per_non_first_launch|Cost per launch|11|
onsite_unique_registration|Unique registration|11|
onsite_cost_per_unique_registration|Cost per unique registration|11|
onsite_registration_rate|Registration rate (%)|11|'''

# Basic-report metrics, part 2.
_BASIC_2 = r'''onsite_total_registration|Total registration|11|
onsite_cost_per_registration|Cost per registration|11|
onsite_unique_ad_impression|Unique ad impression event|11|
onsite_cost_per_unique_ad_impression|Cost per unique ad impression event|11|
onsite_ad_impression_rate|Ad impression event rate|11|
onsite_total_ad_impression|Total ad impression events|11|
onsite_cost_per_ad_impression|Cost per ad impression event|11|
onsite_total_ad_impression_value|Total ad impression value|11|
onsite_value_per_ad_impression|Value per ad impression event|11|
onsite_ad_impression_ad_revenue_calendar_day0|Calendar day 0 ad revenue|11|
onsite_ad_impression_ad_revenue_calendar_day6|Calendar day 6 ad revenue|11|
onsite_ad_impression_ad_revenue_calendar_day13|Calendar day 13 ad revenue|11|
onsite_ad_impression_ad_revenue_roas_calendar_day0|Calendar Day 0 ad revenue ROAS|11|
onsite_ad_impression_ad_revenue_roas_calendar_day6|Calendar Day 6 ad revenue ROAS|11|
onsite_ad_impression_ad_revenue_roas_calendar_day13|Calendar Day 13 ad revenue ROAS|11|
onsite_ad_impression_ad_revenue_day0|Day 0 ad revenue|11|
onsite_ad_impression_ad_revenue_day1|Day 1 ad revenue|11|
onsite_ad_impression_ad_revenue_day2|Day 2 ad revenue|11|
onsite_ad_impression_ad_revenue_day3|Day 3 ad revenue|11|
onsite_ad_impression_ad_revenue_day4|Day 4 ad revenue|11|
onsite_ad_impression_ad_revenue_day5|Day 5 ad revenue|11|
onsite_ad_impression_ad_revenue_day6|Day 6 ad revenue|11|
onsite_ad_impression_ad_revenue_day13|Day 13 ad revenue|11|
onsite_ad_impression_ad_revenue_roas_day0|Day 0 ad revenue ROAS|11|
onsite_ad_impression_ad_revenue_roas_day1|Day 1 ad revenue ROAS|11|
onsite_ad_impression_ad_revenue_roas_day2|Day 2 ad revenue ROAS|11|
onsite_ad_impression_ad_revenue_roas_day3|Day 3 ad revenue ROAS|11|
onsite_ad_impression_ad_revenue_roas_day4|Day 4 ad revenue ROAS|11|
onsite_ad_impression_ad_revenue_roas_day5|Day 5 ad revenue ROAS|11|
onsite_ad_impression_ad_revenue_roas_day6|Day 6 ad revenue ROAS|11|
onsite_ad_impression_ad_revenue_roas_day13|Day 13 ad revenue ROAS|11|
onsite_add_to_wishlist|Adds to wishlist (TikTok)|11|
cost_per_onsite_add_to_wishlist|Cost per add to wishlist (TikTok)|11|
onsite_add_to_wishlist_rate|Unique add to wishlist rate (TikTok)|11|
value_per_onsite_add_to_wishlist|Value per add to wishlist (TikTok)|11|
total_onsite_add_to_wishlist_value|Add to wishlist value (TikTok)|11|
onsite_add_billing|Billing adds (TikTok)|11|
cost_per_onsite_add_billing|Cost per billing add (TikTok)|11|
onsite_add_billing_rate|Billing add rate (TikTok) (%)|11|
value_per_onsite_add_billing|Value per billing add (TikTok)|11|
total_onsite_add_billing_value|Billing add value (TikTok)|11|
onsite_form|Form submissions (TikTok)|11|
cost_per_onsite_form|Cost per form submitted (TikTok)|11|
onsite_form_rate|Unique form submission rate (TikTok)|11|
value_per_onsite_form|Value per form submitted (TikTok)|11|
total_onsite_form_value|Form submission value (TikTok)|11|
onsite_total_filtered_out_form_submission|Filtered out form submissions (TikTok)|11|
onsite_download_start|App store clicks (TikTok)|11|
cost_per_onsite_download_start|Cost per app store click (TikTok)|11|
onsite_download_start_rate|App store click rate (TikTok) (%)|11|
onsite_destination_visits|Destination visits (TikTok)|11|
cost_per_onsite_destination_visit|Cost per destination visit (TikTok)|11|
onsite_destination_visit_rate|Destination visit rate (TikTok) (%)|11|
ix_page_view_count|Page views (TikTok)|11|
cost_per_ix_page_view_count|Cost per page view (TikTok)|11|
ix_page_view_count_rate|Page view rate (TikTok) (%)|11|
ix_button_click_count|Outbound clicks (TikTok)|11|
cost_per_ix_button_click_count|Cost per outbound click (TikTok)|11|
ix_button_click_count_rate|Outbound click rate (TikTok) (%)|11|
ix_product_click_count|Product clicks (TikTok)|11|
cost_per_ix_product_click_count|Cost per product clicks (TikTok)|11|
ix_product_click_count_rate|Product clicks rate (TikTok) (%)|11|
onsite_unique_subscribe|Unique subscriptions (TikTok)|11|
onsite_cost_per_unique_subscribe|Cost per unique subscription (TikTok)|11|
onsite_subscribe_rate|Subscription rate (TikTok) (%)|11|
onsite_total_subscribe|Subscriptions (TikTok)|11|
onsite_cost_per_total_subscribe|Cost per subscription (TikTok)|11|
onsite_value_per_subscribe|Value per subscription (TikTok)|11|
onsite_total_subscribe_value|Subscription value (TikTok)|11|
onsite_subscribe_value_day0|Day 0 subscription value|11|
onsite_subscribe_value_day1|Day 1 subscription value|11|
onsite_subscribe_value_day2|Day 2 subscription value|11|
onsite_subscribe_value_day3|Day 3 subscription value|11|
onsite_subscribe_value_day4|Day 4 subscription value|11|
onsite_subscribe_value_day5|Day 5 subscription value|11|
onsite_subscribe_value_day6|Day 6 subscription value|11|
onsite_subscribe_value_day13|Day 13 subscription value|11|
onsite_subscribe_value_day20|Day 20 subscription value|11|
onsite_subscribe_value_day27|Day 27 subscription value|11|
onsite_subscribe_value_day29|Day 29 subscription value|11|
onsite_ad_impression_ad_revenue_roas|Ad revenue ROAS (TikTok)|11|
native_growth_purchase_roas|Native Growth purchase ROAS (TikTok)|12|
native_growth_total_purchase_event_count|Native Growth total purchase (TikTok)|12|
native_growth_unique_purchase|Native Growth unique purchase (TikTok)|12|
native_growth_cost_per_unique_purchase|Native Growth cost per unique purchase (TikTok)|12|
native_growth_cost_per_purchase|Native Growth cost per purchase (TikTok)|12|
native_growth_purchase_rate|Native Growth purchase rate (TikTok)|12|
native_growth_value_per_purchase|Native Growth value per purchase (TikTok)|12|
native_growth_total_purchase_value|Native Growth total purchase value (TikTok)|12|
native_growth_purchase_event_count_d0|Native Growth day 0 purchase|12|
native_growth_purchase_value_d0|Native Growth day 0 purchase value|12|
native_growth_purchase_value_d1|Native Growth day 1 purchase value|12|
native_growth_purchase_value_d2|Native Growth day 2 purchase value|12|
native_growth_purchase_value_d3|Native Growth day 3 purchase value|12|
native_growth_purchase_value_d4|Native Growth day 4 purchase value|12|
native_growth_purchase_value_d5|Native Growth day 5 purchase value|12|
native_growth_purchase_value_d6|Native Growth day 6 purchase value|12|
native_growth_purchase_value_d13|Native Growth day 13 purchase value|12|
native_growth_purchase_value_d20|Native Growth day 20 purchase value|12|
native_growth_purchase_value_d27|Native Growth day 27 purchase value|12|
native_growth_purchase_value_d29|Native Growth day 29 purchase value|12|
native_growth_purchase_value_d31|Native Growth day 31 purchase value|12|
native_growth_purchase_roas_d0|Native Growth day 0 purchase ROAS|12|
native_growth_purchase_roas_d1|Native Growth day 1 purchase ROAS|12|
native_growth_purchase_roas_d2|Native Growth day 2 purchase ROAS|12|
native_growth_purchase_roas_d3|Native Growth day 3 purchase ROAS|12|
native_growth_purchase_roas_d4|Native Growth day 4 purchase ROAS|12|
native_growth_purchase_roas_d5|Native Growth day 5 purchase ROAS|12|
native_growth_purchase_roas_d6|Native Growth day 6 purchase ROAS|12|
native_growth_purchase_roas_d13|Native Growth day 13 purchase ROAS|12|
native_growth_purchase_roas_d20|Native Growth day 20 purchase ROAS|12|
native_growth_purchase_roas_d27|Native Growth day 27 purchase ROAS|12|
native_growth_purchase_roas_d29|Native Growth day 29 purchase ROAS|12|
native_growth_purchase_roas_d31|Native Growth day 31 purchase ROAS|12|
native_growth_active_by_conversion_time|Native Growth active by conversion time (TikTok)|12|
native_growth_cost_per_active_by_conversion_time|Native Growth cost per active by conversion time (TikTok)|12|
native_growth_active|Native Growth active (TikTok)|12|
native_growth_cost_per_active|Native Growth cost per active (TikTok)|12|
native_growth_unique_launch_app|Native Growth unique launch|12|
native_growth_cost_per_unique_launch_app|Native Growth cost per unique launch|12|
native_growth_launch_rate|Native Growth launch Rate (%)|12|
native_growth_total_launch_event_count|Native Growth total launch|12|
native_growth_cost_per_launch_app|Native Growth cost per launch|12|
native_growth_unique_ad_impression_event|Native Growth unique ad impression event|12|
native_growth_cost_per_unique_ad_impression_event|Native Growth cost per unique ad impression event|12|
native_growth_ad_impression_event_rate|Native Growth ad impression event rate|12|
native_growth_total_ad_impression_event_count|Native Growth total ad impression events|12|
native_growth_cost_per_ad_impression_event|Native Growth cost per ad impression event|12|
native_growth_total_ad_impression_value|Native Growth total ad impression value|12|
native_growth_value_per_ad_impression_event|Native Growth value per ad impression event|12|
native_growth_ad_revenue_roas|Native Growth ad revenue ROAS|12|
native_growth_ad_revenue_event_count_d0|Native Growth day 0 ad revenue|12|
native_growth_ad_revenue_value_d0|Native Growth day 0 ad revenue value|12|
native_growth_ad_revenue_value_d1|Native Growth day 1 ad revenue value|12|
native_growth_ad_revenue_value_d2|Native Growth day 2 ad revenue value|12|
native_growth_ad_revenue_value_d3|Native Growth day 3 ad revenue value|12|
native_growth_ad_revenue_value_d4|Native Growth day 4 ad revenue value|12|
native_growth_ad_revenue_value_d5|Native Growth day 5 ad revenue value|12|
native_growth_ad_revenue_value_d6|Native Growth day 6 ad revenue value|12|
native_growth_ad_revenue_value_d13|Native Growth day 13 ad revenue value|12|
native_growth_ad_revenue_value_d20|Native Growth day 20 ad revenue value|12|
native_growth_ad_revenue_value_d27|Native Growth day 27 ad revenue value|12|
native_growth_ad_revenue_value_d29|Native Growth day 29 ad revenue value|12|
native_growth_ad_revenue_value_d31|Native Growth day 31 ad revenue value|12|
native_growth_ad_revenue_roas_d0|Native Growth day 0 ad revenue ROAS|12|
native_growth_ad_revenue_roas_d1|Native Growth day 1 ad revenue ROAS|12|
native_growth_ad_revenue_roas_d2|Native Growth day 2 ad revenue ROAS|12|
native_growth_ad_revenue_roas_d3|Native Growth day 3 ad revenue ROAS|12|
native_growth_ad_revenue_roas_d4|Native Growth day 4 ad revenue ROAS|12|
native_growth_ad_revenue_roas_d5|Native Growth day 5 ad revenue ROAS|12|
native_growth_ad_revenue_roas_d6|Native Growth day 6 ad revenue ROAS|12|
native_growth_ad_revenue_roas_d13|Native Growth day 13 ad revenue ROAS|12|
native_growth_ad_revenue_roas_d20|Native Growth day 20 ad revenue ROAS|12|
native_growth_ad_revenue_roas_d27|Native Growth day 27 ad revenue ROAS|12|
native_growth_ad_revenue_roas_d29|Native Growth day 29 ad revenue ROAS|12|
native_growth_ad_revenue_roas_d31|Native Growth day 31 ad revenue ROAS|12|
native_growth_unique_subscribe_count|Native Growth unique subscribe|12|
native_growth_cost_per_unique_subscribe|Native Growth cost per unique subscribe|12|
native_growth_subscribe_rate|Native Growth subscribe rate (%)|12|
native_growth_total_subscribe_event_count|Native Growth total subscribe|12|
native_growth_cost_per_subscribe|Native Growth cost per subscribe|12|
native_growth_value_per_subscribe|Native Growth value per subscribe|12|
native_growth_total_subscribe_value|Native Growth total subscribe value|12|
native_growth_subscribe_value_d0|Native Growth day 0 subscribe value|12|
native_growth_subscribe_value_d1|Native Growth day 1 subscribe value|12|
native_growth_subscribe_value_d2|Native Growth day 2 subscribe value|12|
native_growth_subscribe_value_d3|Native Growth day 3 subscribe value|12|
native_growth_subscribe_value_d4|Native Growth day 4 subscribe value|12|
native_growth_subscribe_value_d5|Native Growth day 5 subscribe value|12|
native_growth_subscribe_value_d6|Native Growth day 6 subscribe value|12|
native_growth_subscribe_value_d13|Native Growth day 13 subscribe value|12|
native_growth_subscribe_value_d20|Native Growth day 20 subscribe value|12|
native_growth_subscribe_value_d27|Native Growth day 27 subscribe value|12|
native_growth_subscribe_value_d29|Native Growth day 29 subscribe value|12|
native_growth_subscribe_value_d31|Native Growth day 31 subscribe value|12|
offline_total_crm_events|CRM events|13|
offline_cost_per_crm_event|Cost per CRM Event|13|
offline_crm_event_rate|CRM Event Rate (%)|13|
offline_value_per_crm_event|Value per CRM Event|13|
offline_crm_event_value|CRM event value|13|
offline_shopping_events|Purchases (offline)|13|
cost_per_offline_shopping_event|Cost per purchase (offline)|13|
offline_shopping_event_rate|Unique purchase rate (offline) (%)|13|
value_per_offline_shopping_event|Value per purchase (offline)|13|
offline_shopping_events_value|Purchase value (offline)|13|
offline_shopping_events_roas|Purchase ROAS (offline)|13|
offline_contact_events|Contacts (offline)|13|
cost_per_offline_contact_event|Cost per contact (offline)|13|
offline_contact_event_rate|Unique contact rate (offline) (%)|13|
value_per_offline_contact_event|Value per contact (offline)|13|
offline_contact_events_value|Contact value (offline)|13|
offline_subscribe_events|Subscriptions (offline)|13|
cost_per_offline_subscribe_event|Cost per subscription (offline)|13|
offline_subscribe_event_rate|Unique subscription rate (offline) (%)|13|
value_per_offline_subscribe_event|Value per subscription (offline)|13|
offline_subscribe_events_value|Subscription value (offline)|13|
offline_form_events|Form submissions (offline)|13|
cost_per_offline_form_event|Cost per form submitted (offline)|13|
offline_form_event_rate|Unique form submission rate (offline) (%)|13|
value_per_offline_form_event|Value per form submitted (offline)|13|
offline_form_events_value|Form submission value (offline)|13|
offline_add_payment_info_events|Payment info adds (offline)|13|
cost_per_offline_add_payment_info_event|Cost per payment info add (offline)|13|
offline_add_payment_info_event_rate|Unique payment info add rate (offline) (%)|13|
value_per_offline_add_payment_info_event|Value per payment info add (offline)|13|
offline_add_payment_info_events_value|Payment info add value (offline)|13|
offline_add_to_cart_events|Adds to cart (offline)|13|
cost_per_offline_add_to_cart_event|Cost per add to cart (offline)|13|
offline_add_to_cart_event_rate|Unique add to cart rate (offline) (%)|13|
value_per_offline_add_to_cart_event|Value per add to cart (offline)|13|
offline_add_to_cart_events_value|Add to cart value (offline)|13|
offline_add_to_wishlist_events|Adds to wishlist (offline)|13|
cost_per_offline_add_to_wishlist_event|Cost per add to wishlist (offline)|13|
offline_add_to_wishlist_event_rate|Unique add to wishlist rate (offline) (%)|13|
value_per_offline_add_to_wishlist_event|Value per add to wishlist (offline)|13|
offline_add_to_wishlist_events_value|Add to wishlist value (offline)|13|
offline_click_button_events|Button clicks (offline)|13|
cost_per_offline_click_button_event|Cost per button click (offline)|13|
offline_click_button_event_rate|Unique button click rate (offline) (%)|13|
value_per_offline_click_button_event|Value per button click (offline)|13|
offline_click_button_events_value|Button click value (offline)|13|
offline_complete_registration_events|Registrations (offline)|13|
cost_per_offline_complete_registration_event|Cost per registration (offline)|13|
offline_complete_registration_event_rate|Unique registration rate (offline) (%)|13|
value_per_offline_complete_registration_event|Value per registration (offline)|13|
offline_complete_registration_events_value|Registration value (offline)|13|
offline_download_events|Downloads (offline)|13|
cost_per_offline_download_event|Cost per download (offline)|13|
offline_download_event_rate|Unique download rate (offline) (%)|13|
value_per_offline_download_event|Value per download (offline)|13|
offline_download_events_value|Download value (offline)|13|
offline_initiate_checkout_events|Checkouts initiated (offline)|13|
cost_per_offline_initiate_checkout_event|Cost per checkout initiated (offline)|13|
offline_initiate_checkout_event_rate|Unique checkout initiation rate (offline) (%)|13|
value_per_offline_initiate_checkout_event|Value per checkout initiated (offline)|13|
offline_initiate_checkout_events_value|Checkout initiation value (offline)|13|
offline_place_order_events|Orders placed (offline)|13|
cost_per_offline_place_order_event|Cost per order placed (offline)|13|
offline_place_order_event_rate|Unique orders placed rate (offline) (%)|13|
value_per_offline_place_order_event|Value per order placed (offline)|13|
offline_place_order_events_value|Orders placed value (offline)|13|
offline_search_events|Searches (offline)|13|
cost_per_offline_search_event|Cost per search (offline)|13|
offline_search_event_rate|Unique search rate (offline) (%)|13|
value_per_offline_search_event|Value per search (offline)|13|
offline_search_events_value|Search value (offline)|13|
offline_view_content_events|Content views (offline)|13|
cost_per_offline_view_content_event|Cost per content view (offline)|13|
offline_view_content_event_rate|Unique content view rate (offline) (%)|13|
value_per_offline_view_content_event|Value per content view (offline)|13|
offline_view_content_events_value|Content view value (offline)|13|
offline_preferred_leads|Preferred leads (offline)|13|
offline_cost_per_preferred_lead|Cost per preferred leads (offline)|13|
offline_preferred_lead_rate|Preferred leads rate (offline) (%)|13|
offline_total_find_location|Total find location (offline)|13|
offline_cost_per_find_location|Cost per find location (offline)|13|
offline_find_location_rate|Find location rate (offline)|13|
offline_value_per_find_location|Value per find location (offline)|13|
offline_total_find_location_value|Total find location value (offline)|13|
offline_total_schedule|Total schedule (offline)|13|
offline_cost_per_schedule|Cost per schedule (offline)|13|
offline_schedule_rate|Schedule rate (offline)|13|
offline_value_per_schedule|Value per schedule (offline)|13|
offline_total_schedule_value|Total schedule value (offline)|13|
offline_total_customize_product|Total customize product (offline)|13|
offline_cost_per_customize_product|Cost per customize product (offline)|13|
offline_customize_product_rate|Customize product rate (offline)|13|
offline_value_per_customize_product|Value per customize product (offline)|13|
offline_total_customize_product_value|Total customize product value (offline)|13|
offline_total_custom_events|Custom events (offline)|13|
offline_cost_per_custom_event|Cost per custom event (offline)|13|
offline_custom_event_rate|Custom event rate (offline) (%)|13|
offline_value_per_custom_event|Value per custom event (offline)|13|
offline_total_custom_event_value|Custom event value (offline)|13|
messaging_total_conversation_tiktok_direct_message|Conversations (TikTok direct message)|14|
messaging_cost_per_conversation_tiktok_direct_message|Cost per conversation (TikTok direct message)|14|
messaging_conversation_rate_tiktok_direct_message|Conversation rate (TikTok direct message) (%)|14|
messaging_total_conversation_instant_messaging_app|Conversations (Instant messaging app)|14|
messaging_cost_per_conversation_instant_messaging_app|Cost per conversation (Instant messaging app)|14|
messaging_conversation_rate_instant_messaging_app|Conversation rate (Instant messaging app) (%)|14|
onsite_shopping_roas|ROAS (Shop)|15|
onsite_shopping|Purchases (Shop)|15|
cost_per_onsite_shopping|Cost per purchase (Shop)|15|
onsite_shopping_rate|Purchase rate (Shop)|15|
value_per_onsite_shopping|Average order value (Shop)|15|
total_onsite_shopping_value|Gross revenue (Shop)|15|
shop_total_purchase_by_order_submission|Purchases (Shop) by order submission time|15|
shop_gross_revenue_by_order_submission|Gross revenue (Shop) by order submission time|15|
shop_total_items_purchased|Items purchased (Shop)|15|
onsite_initiate_checkout_count|Checkouts initiated (Shop)|15|
cost_per_onsite_initiate_checkout_count|Cost per checkout initiated (Shop)|15|
onsite_initiate_checkout_count_rate|Checkout initiation rate (Shop)|15|
value_per_onsite_initiate_checkout_count|Value per checkout initiated (Shop)|15|
total_onsite_initiate_checkout_count_value|Checkout initiation value (Shop)|15|
onsite_on_web_detail|Product page views (Shop)|15|
cost_per_onsite_on_web_detail|Cost per product page view (Shop)|15|
onsite_on_web_detail_rate|Product page view rate (Shop) (%)|15|
value_per_onsite_on_web_detail|Value per product page view (Shop)|15|
total_onsite_on_web_detail_value|Product page view value (Shop)|15|
onsite_on_web_cart|Add to cart (Shop)|15|
cost_per_onsite_on_web_cart|Cost per add to cart (Shop)|15|
onsite_on_web_cart_rate|Add to cart rate (Shop) (%)|15|
value_per_onsite_on_web_cart|Value per add to cart (Shop)|15|
total_onsite_on_web_cart_value|Add to cart value (Shop)|15|
vta_conversion|VTA conversions|16|
cost_per_vta_conversion|Cost per VTA conversion|16|
cta_conversion|CTA conversions|16|
cost_per_cta_conversion|Cost per CTA conversion|16|
engaged_view_through_conversions|EVTA conversions|16|
cost_per_engaged_view_through_conversion|Cost per EVTA conversion|16|
vta_app_install|VTA app installs|16|
cost_per_vta_app_install|Cost per VTA app install|16|
cta_app_install|CTA app installs|16|
cost_per_cta_app_install|Cost per CTA app install|16|
evta_app_install|EVTA app installs|16|
cost_per_evta_app_install|Cost per EVTA app install|16|
vta_registration|VTA registrations|16|
cost_per_vta_registration|Cost per VTA registration|16|
cta_registration|CTA registrations|16|
cost_per_cta_registration|Cost per CTA registration|16|
evta_registration|EVTA registrations|16|
cost_per_evta_registration|Cost per EVTA registration|16|
vta_purchase|VTA purchases (app)|16|
cost_per_vta_purchase|Cost per VTA purchase (app)|16|
cta_purchase|CTA purchases (app)|16|
cost_per_cta_purchase|Cost per CTA purchase (app)|16|
evta_purchase|EVTA purchases (app)|16|
cost_per_evta_purchase|Cost per EVTA purchase (app)|16|
vta_complete_payment|VTA purchases (website)|16|
cost_per_vta_payments_completed|Cost per VTA purchase (website)|16|
evta_payments_completed|EVTA purchases (website)|16|
cost_per_evta_payments_completed|Cost per EVTA purchase (website)|16|
vta_complete_payment_value|VTA purchase value|16|
vta_complete_payment_roas|VTA purchase ROAS|16|
skan_result|Number of times your ad resulted in an intended outcome based on your campaign objective and optimization goal.|17|
skan_cost_per_result|Average cost per each result from your ads.|17|
skan_result_rate|Percentage of results that happened out of all impressions on your ads.|17|
skan_conversion|Number of times your ad resulted in an intended outcome based on your optimization event.|17|
skan_cost_per_conversion|Average amount you spent on a conversion.|17|
skan_conversion_rate|Percentage of conversions you received out of all destination clicks on your ads.|17|
skan_conversion_rate_v2|Percentage of conversions you received out of all impressions on your ads.|17|
skan_click_time_conversion|The number of times your ad achieved an outcome based on the objective and settings you selected|17|
skan_click_time_cost_per_conversionn|The average amount of money you've spent on a conversion|17|
skan_click_time_conversion_rate|The percentage of conversions from all clicks of your ads|17|
skan_click_time_conversion_rate_v2|The percentage of conversions from all impressions on your ads|17|
skan_app_install|Number of unique app install events attributed to your ads.|18|
skan_cost_per_app_install|Average cost per unique app install event|18|
skan_app_install_withheld|Number of unique app install events attributed to your ads, with withheld values due to Apple's privacy threshold.|18|
skan_registration|Number of unique registration events attributed to your ads|18|
skan_cost_per_registration|Average cost per unique registration event|18|
skan_registration_rate|Percentage of unique registration events out of all app install events|18|
skan_total_registration|Number of in-app registration events attributed to your ads|18|
skan_cost_per_total_registration|Average cost per registration event|18|
skan_purchase|Number of unique purchase events attributed to your ads|18|
skan_cost_per_purchase|Average cost per unique purchase event|18|
skan_purchase_rate|Percentage of unique purchase events out of all app install events|18|
skan_total_purchase|Number of purchase events attributed to your ads|18|
skan_cost_per_total_purchase|Average cost per purchase event|18|
skan_total_purchase_value|Total value returned from all purchase events|18|
skan_total_repetitive_active_pay_roas|Total return on ad spend (ROAS) from purchase events|18|
skan_add_to_cart|Number of unique add to cart events attributed to your ads|18|
skan_cost_per_add_to_cart|Average cost per unique add to cart event|18|
skan_add_to_cart_rate|Percentage of unique add to cart events out of all app install events|18|
skan_total_add_to_cart|Number of add to cart events attributed to your ads|18|
skan_cost_per_total_add_to_cart|Average cost per add to cart event|18|
skan_total_add_to_cart_value|Total value returned from all add to cart events|18|
skan_checkout|Number of unique checkout events attributed to your ads|18|
skan_cost_per_checkout|Average cost per unique checkout event|18|
skan_checkout_rate|Percentage of unique checkout events out of all app install events|18|
skan_total_checkout|Number of checkout events attributed to your ads|18|
skan_cost_per_total_checkout|Average cost per checkout event|18|
skan_total_checkout_value|Total value returned from all checkout events|18|
skan_view_content|Number of unique view content events attributed to your ads|18|
skan_cost_per_view_content|Average cost per unique view content event|18|
skan_view_content_rate|Percentage of unique view content events out of all app install events|18|
skan_total_view_content|Number of view content events attributed to your ads|18|
skan_cost_per_total_view_content|Average cost per view content event|18|
skan_total_view_content_value|Total value returned from all view content events|18|
skan_add_payment_info|Number of unique add payment info events attributed to your ads|18|
skan_cost_per_add_payment_info|Average cost per unique add payment info event|18|
skan_add_payment_info_rate|Percentage of unique add payment info events out of all app install events|18|
skan_total_add_payment_info|Number of add payment info events attributed to your ads|18|
skan_cost_total_add_payment_info|Average cost per add payment info event|18|
skan_add_to_wishlist|Number of unique add to wishlist events attributed to your ads|18|
skan_cost_per_add_to_wishlist|Average cost per unique add to wishlist event|18|
skan_add_to_wishlist_rate|Percentage of unique add to wishlist events out of all app install events|18|
skan_total_add_to_wishlist|Number of add to wishlist events attributed to your ads|18|
skan_cost_per_total_add_to_wishlist|Average cost per add to wishlist event|18|
skan_total_add_to_wishlist_value|Total value returned from all add to wishlist events|18|
skan_launch_app|Number of unique launch app events attributed to your ads|18|
skan_cost_per_launch_app|Average cost per unique launch app event|18|
skan_launch_app_rate|Percentage of unique launch app events out of all app install events|18|
skan_total_launch_app|Number of launch app events attributed to your ads|18|
skan_cost_per_total_launch_app|Average cost per launch app event|18|
skan_complete_tutorial|Number of unique complete tutorial events attributed to your ads|18|
skan_cost_per_complete_tutorial|Average cost per unique complete tutorial event|18|
skan_complete_tutorial_rate|Percentage of unique complete tutorial events out of all app install events|18|
skan_total_complete_tutorial|Number of complete tutorial events attributed to your ads|18|
skan_cost_per_total_complete_tutorial|Average cost per complete tutorial event|18|
skan_total_complete_tutorial_value|Total value returned from all complete tutorial events|18|
skan_create_group|Number of unique create group events attributed to your ads|18|
skan_cost_per_create_group|Average cost per unique create group event|18|
skan_create_group_rate|Percentage of unique create group events out of all app install events|18|
skan_total_create_group|Number of create group events attributed to your ads|18|
skan_cost_per_total_create_group|Average cost per create group event|18|
skan_total_create_group_value|Total value returned from all create group events|18|
skan_join_group|Number of unique join group events attributed to your ads|18|
skan_cost_per_join_group|Average cost per unique join group event|18|
skan_join_group_rate|Percentage of unique join group events out of all app install events|18|
skan_total_join_group|Number of join group events attributed to your ads|18|
skan_cost_per_total_join_group|Average cost per join group event|18|
skan_total_join_group_value|Total value returned from all join group events|18|
skan_create_gamerole|Number of unique create role events attributed to your ads|18|
skan_cost_per_create_gamerole|Average cost per unique create role event|18|
skan_create_gamerole_rate|Percentage of unique create role events out of all app install events|18|
skan_total_create_gamerole|Number of create role events attributed to your ads|18|
skan_cost_per_total_create_gamerole|Average cost per create role event|18|
skan_total_create_gamerole_value|Total value returned from all create role events|18|
skan_spend_credits|Number of unique spend credit events attributed to your ads|18|
skan_cost_per_spend_credits|Average cost per unique spend credit event|18|
skan_spend_credits_rate|Percentage of unique spend credit events out of all app install events|18|
skan_total_spend_credits|Number of spend credit events attributed to your ads|18|
skan_cost_per_total_spend_credits|Average cost per spend credit event|18|
skan_total_spend_credits_value|Total value returned from all spend credit events|18|
skan_achieve_level|Number of unique achieve level events attributed to your ads|18|
skan_cost_per_achieve_level|Average cost per unique achieve level event|18|
skan_achieve_level_rate|Percentage of unique achieve level events out of all app install events|18|
skan_total_achieve_level|Number of achieve level events attributed to your ads|18|
skan_cost_per_total_achieve_level|Average cost per achieve level event|18|
skan_total_achieve_level_value|Total value returned from all achieve level events|18|
skan_unlock_achievement|Number of unique unlock achievement events attributed to your ads|18|
skan_cost_per_unlock_achievement|Average cost per unique unlock achievement event|18|
skan_unlock_achievement_rate|Percentage of unique unlock achievement events out of all app install events|18|
skan_total_unlock_achievement|Number of unlock achievement events attributed to your ads|18|
skan_cost_per_total_unlock_achievement|Average cost per unlock achievement event|18|
skan_total_unlock_achievement_value|Total value returned from all unlock achievement events|18|
skan_sales_lead|Number of unique generate lead events attributed to your ads|18|
skan_cost_per_sales_lead|Average cost per unique generate lead event|18|
skan_sales_lead_rate|Percentage of unique generate lead events out of all app install events|18|
skan_total_sales_lead|Number of generate lead events attributed to your ads|18|
skan_cost_per_total_sales_lead|Average cost per generate lead event|18|
skan_total_sales_lead_value|Total value returned from all generate lead events|18|
skan_in_app_ad_click|Number of unique in-app ad click events attributed to your ads|18|
skan_cost_per_in_app_ad_click|Average cost per unique in-app ad click event|18|
skan_in_app_ad_click_rate|Percentage of unique in-app ad click events out of all app install events|18|
skan_total_in_app_ad_click|Number of in-app ad click events attributed to your ads|18|
skan_cost_per_total_in_app_ad_click|Average cost per in-app ad click event|18|
skan_total_in_app_ad_click_value|Total value returned from all in-app ad click events|18|
skan_in_app_ad_impr|Number of unique in-app ad impression events attributed to your ads|18|
skan_cost_per_in_app_ad_impr|Average cost per unique in-app ad impression event|18|
skan_in_app_ad_impr_rate|Percentage of unique in-app ad impression events out of all app install events|18|
skan_total_in_app_ad_impr|Number of in-app ad impression events attributed to your ads|18|
skan_cost_per_total_in_app_ad_impr|Average cost per in-app ad impression event|18|
skan_total_in_app_ad_impr_value|Total value returned from all in-app ad impression events|18|
skan_loan_apply|Number of unique loan approval events attributed to your ads|18|
skan_cost_per_loan_apply|Average cost per unique loan approval event|18|
skan_loan_apply_rate|Percentage of unique loan approval events out of all app install events|18|
skan_total_loan_apply|Number of loan approval events attributed to your ads|18|
skan_cost_per_total_loan_apply|Average cost per loan approval event|18|
skan_loan_credit|Number of unique loan approval events attributed to your ads|18|
skan_cost_per_loan_credit|Average cost per unique loan approval event|18|
skan_loan_credit_rate|Percentage of unique loan approval events out of all app install events|18|
skan_total_loan_credit|Number of loan approval events attributed to your ads|18|
skan_cost_per_total_loan_credit|Average cost per loan approval event|18|
skan_loan_disbursement|Number of unique loan disbursal events attributed to your ads|18|
skan_cost_per_loan_disbursement|Average cost per unique loan disbursal event|18|
skan_loan_disbursement_rate|Percentage of unique loan disbursal events out of all app install events|18|
skan_total_loan_disbursement|Number of loan disbursal events attributed to your ads|18|
skan_cost_per_total_loan_disbursement|Average cost per loan disbursal event|18|
skan_login|Number of unique login events attributed to your ads|18|
skan_cost_per_login|Average cost per unique login event|18|
skan_login_rate|Percentage of unique login events out of all app install events|18|
skan_total_login|Number of login events attributed to your ads|18|
skan_cost_per_total_login|Average cost per login event|18|
skan_ratings|Number of unique rate events attributed to your ads|18|
skan_cost_per_ratings|Average cost per unique rate event|18|
skan_ratings_rate|Percentage of unique rate events out of all app install events|18|
skan_total_ratings|Number of rate events attributed to your ads|18|
skan_cost_per_total_ratings|Average cost per rate event|18|
skan_total_ratings_value|Total value returned from all rate events|18|
skan_search|Number of unique search events attributed to your ads|18|
skan_cost_per_search|Average cost per unique search event|18|
skan_search_rate|Percentage of unique search events out of all app install events|18|
skan_total_search|Number of search events attributed to your ads|18|
skan_cost_per_total_search|Average cost per search event|18|
skan_start_trial|Number of unique start trial events attributed to your ads|18|
skan_cost_per_start_trial|Average cost per unique start trial event|18|
skan_start_trial_rate|Percentage of unique start trial events out of all app install events|18|
skan_total_start_trial|Number of start trial events attributed to your ads|18|
skan_cost_per_total_start_trial|Average cost per start trial event|18|
skan_subscribe|Number of unique subscribe events attributed to your ads|18|
skan_cost_per_subscribe|Average cost per unique subscribe event|18|
skan_subscribe_rate|Percentage of unique subscribe events out of all app install events|18|
skan_total_subscribe|Number of subscribe events attributed to your ads|18|
skan_cost_per_total_subscribe|Average cost per subscribe event|18|
skan_total_subscribe_value|Total value returned from all subscribe events|18|
skan_vta_conversion|Number of conversion events attributed to ad impressions|19|
skan_cost_per_vta_conversion|Average cost per conversion event attributed to an ad impression|19|
skan_vta_app_install|Number of app install events attributed to ad impressions|19|
skan_cost_per_vta_app_install|Average cost per app install event attributed to an ad impression|19|
skan_vta_registration|Number of in-app registration events attributed to an ad impression|19|
skan_cost_per_vta_registration|Average cost per in-app registration event attributed to an ad impression|19|
skan_vta_purchase|Number of in-app purchase events attributed to ad impressions|19|
skan_cost_per_vta_purchase|Average cost per in-app purchase event attributed to an ad impression|19|
skan_cta_conversion|Number of in-app conversion events attributed to ad impressions|19|
skan_cost_per_cta_conversion|Average cost per in-app conversion event attributed to a click on your ad|19|
skan_cta_app_install|Number of app install events attributed to clicks on your ad|19|
skan_cost_per_cta_app_install|Average cost per app install event attributed to a click on your ad|19|
skan_cta_registration|Number of in-app registration events attributed to a click on your ad|19|
skan_cost_per_cta_registration|Average cost per in-app registration event attributed to a click on your ad|19|
skan_cta_purchase|Number of in-app purchase events attributed to clicks on your ad|19|
skan_cost_per_cta_purchase|Average cost per in-app purchase event attributed to a click on your ad|19|'''

BASIC = tuple(
    (n, lab, int(g), tuple(f for f in fl.split(",") if f))
    for n, lab, g, fl in (line.split("|") for line in
                          (_BASIC_1 + "\n" + _BASIC_2).splitlines()
                          if line.strip()))
