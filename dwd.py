public_public_futu_au_user_channel_fact_df_sql="""
CREATE TABLE dim.public_public_futu_au_user_channel_fact_df(
    user_id string    COMMENT '用户id;取自dws.user.au_channel_info的uid' ,
    kpi_channel string    COMMENT 'kpi一级渠道;取自dws.user.au_channel_info的type，当type=1->"渠道合作"；当type=2->"ESOP-公司"；当type=3->"流量投放"；当type=4->"应用商店"；当type=5->"其他"；当type=6->"邀请裂变"；其余情况 ->"自然流量"；' ,
    kpi_subchannel string    COMMENT 'kpi二级渠道;取自promote_positions表的promote_name,如果为null，默认为"自然流量"' ,
    reg_channel bigint(20)    COMMENT '注册渠道;取自dws.user.au_channel_info的promote_id' ,
    reg_subchannel bigint(20)    COMMENT '注册子渠道;取自dws.user.au_channel_info的sub_promote_id' ,
    reg_channel_name string    COMMENT '注册渠道名称;取自promote_positions表的promote_name,如果为null，默认为"自然流量"' ,
    reg_subchannel_name string    COMMENT '注册子渠道名称;取自sub_promote_positions表的sub_promote_name,如果为null，默认为"自然流量"' ,
    keyword_id string    COMMENT '关键id;取自dws.user.au_channel_info的keyword_id' ,
    source_id bigint(20)    COMMENT '渠道信息获取来源;取自dws.user.au_channel_info的source_id10：kol 11：asm 12：branch 13.invite 14.auth' ,
    update_time bigint(20)    COMMENT '更新时间;取自dws.user.au_channel_info的update_time,转为秒'
)  COMMENT = 'AU用户渠道信息表';
"""

public_public_futu_au_user_identity_fact_df_sql="""
CREATE TABLE dim.public_public_futu_au_user_identity_fact_df(
    user_id string    COMMENT '用户id;取自两个表full join后的uid、id' ,
    user_type_cw string    COMMENT '用户类型;枚举值：华人、西人、其他；when moomoo_preference_lang=0 or 1 or nation_info=中国大陆(CN)or中国台湾(CT)or中国香港(HK)or中国澳门(MO)则为'Chinese’；when moomoo_preference_lang and nation_info is null 为"其他"；其余情况为"西人"' ,
    nation_info string    COMMENT '国籍;取自客户表的nation_info' ,
    moomoo_preference_lang string    COMMENT '语言偏好;取自用户特征表的moomoo_preference_lang' 
)  COMMENT = 'AU用户身份信息表';

"""


public_public_futu_au_user_channel_fact_df=""" --dim层au用户渠道信息表
select
    a.uid user_id,
    case a.type when 1 then '渠道合作'
        when 2 then 'ESOP-公司'
        when 3 then '流量投放'
        when 4 then '应用商店'
        when 5 then '其他'
        when 6 then '邀请裂变'
        else '自然流量' end as kpi_channel,
    nv1(b.name,'自然流量') kpi_subchannel,
    a.promote_id reg_channel,
    a.sub_promote_id reg_subchannel,
    nv1(b.name,'自然流量') reg_channel_name,
    nv1(c.name,'自然流量') reg_subchannel_name,
    keyword_id,
    source_id,
    cast(update_time_ms/1000 as bigint) update_time 
from dws.user_au_channel_info a 
left join moomoo_sg_ad_customer_manage_cm_promote_positions b on a.promote_id=b.id 
left join moomoo_sg_ad_customer_manage_cm_promote_sub_positions c on a.promote_id=c.position_id 
and a.sub_promote_id=c.sub_position_id
"""

public_public_futu_au_user_identity_fact_df=""" --dim层au用户身份信息表
select 
    a.uid user_id,
    case when moomoo_preference_lang=0 or moomoo_preference_lang=1 or nation_info in ('CN','CT','HK",'MO') then '华人'
         when moomoo_preference_lang is null and nation_info is null then '其他'
         else '西人' end as user_type_cw，
    nation_info,
    moomoo_preference_lang 
from dws.t_portrait_{pt_date} a full outer join futu_au_au_user_web_account_setup_order b on a.uid=b.id
"""





