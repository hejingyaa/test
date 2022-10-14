finance_assets_futu_au_assets_assets_daily_df_sql="""
CREATE TABLE dws.finance_assets_futu_au_assets_assets_daily_df(
    date string    COMMENT '日期;取自user_assets_detail_daily表对应字段' ,
    assets_g0_user bigint(20)    COMMENT '总有资产客户数;count(distinct user_id) where total_assets>0' ,
    assets_g0_user_new bigint(20)    COMMENT '新增有资产客户数;count(distinct user_id) where total_assets>0 and assets_g0_lifecycle='new'' ,
    assets_g0_user_back bigint(20)    COMMENT '回流有资产客户数;count(distinct user_id) where total_assets>0 and assets_g0_lifecycle='back'' ,
    assets_g0_user_loss bigint(20)    COMMENT '流失有资产客户数;count(distinct user_id) where total_assets>0 and assets_g0_lifecycle='loss'' ,
    assets_g0_user_net bigint(20)    COMMENT '净增有资产客户数;=本日total_assets_g0_user 减去 上一日 total_assets_g0_user' ,
    kpi_channel string    COMMENT 'kpi一级渠道' ,
    kpi_ subchannel string    COMMENT 'kpi二级渠道' ,
    reg_channel bigint(20)    COMMENT '注册渠道' ,
    reg_subchannel bigint(20)    COMMENT '注册子渠道' ,
    reg_channel_name string    COMMENT '注册渠道名称' ,
    reg_subchannel_name string    COMMENT '注册子渠道名称' ,
    user_type_cw string    COMMENT '用户类型'
)  COMMENT = '每日资产用户数';
"""

finance_assets_futu_au_assets_indicator_statis_m2d_df_sql="""
CREATE TABLE dws.finance_assets_futu_au_assets_indicator_statis_m2d_df(
    date string    COMMENT '日期;若无特殊说明，均取自assets.assets_daily_df表对应字段，取pt_date的自然月' ,
    total_assets_g0_user_new_m bigint(20)    COMMENT '当日月累计新增有资产客户数;sum(assets_g0_user_new)' ,
    total_assets_g0_user_net_m bigint(20)    COMMENT '当日月累计净增有资产客户数;sum(assets_g0_user_net)' ,
    total_assets_g0_user_loss_m bigint(20)    COMMENT '当日月流失有资产客户数;取自当天user_assets_detail_daily表count（distinct user_id）where total_assets<=0 and 该用户上个月最后一天total_assets>0' ,
    total_assets_g0_user_back_m bigint(20)    COMMENT '当日月回流有资产客户数;取自当天user_assets_detail_daily表count（distinct user_id）where total_assets>0 and 该用户上月最后一天total_assets<=0 and 当天date！=first total_assetsg0_date' ,
    kpi_channel string    COMMENT 'kpi一级渠道' ,
    kpi_ subchannel string    COMMENT 'kpi二级渠道' ,
    reg_channel bigint(20)    COMMENT '注册渠道' ,
    reg_subchannel bigint(20)    COMMENT '注册子渠道' ,
    reg_channel_name string    COMMENT '注册渠道名称' ,
    reg_subchannel_name string    COMMENT '注册子渠道名称' ,
    user_type_cw string    COMMENT '用户类型' 
)  COMMENT = '每日当月累计资产用户数';
"""

finance_assets_futu_au_assets_indicator_statis_q2d_df_sql="""
CREATE TABLE dws.finance_assets_futu_au_assets_indicator_statis_q2d_df(
    date string    COMMENT '日期;若无特殊说明，均取自assets.assets_daily_df表对应字段，取pt_date的自然季度' ,
    total_assets_g0_user_new_q bigint(20)    COMMENT '当日季累计新增有资产客户数;sum(assets_g0_user_new)' ,
    total_assets_g0_user_net_q bigint(20)    COMMENT '当日季累计净增有资产客户数;sum(assets_g0_user_net)' ,
    total_assets_g0_user_loss_q bigint(20)    COMMENT '当日季流失有资产客户数;取自当天user_assets_detail_dailyf表count（distinct user_id）where total_assets<=0 and 该用户上个季度最后一天total_assets>0' ,
    total_assets_g0_user_back_q bigint(20)    COMMENT '当日季回流有资产客户数;取自当天user_assets_detail_daily表count（distinct user_id）where total_assets>0 and 该用户上季度最后一天total_assets<=0 and 当天date！=first total_assetsg0_date' ,
    kpi_channel string    COMMENT 'kpi一级渠道' ,
    kpi_ subchannel string    COMMENT 'kpi二级渠道' ,
    reg_channel bigint(20)    COMMENT '注册渠道' ,
    reg_subchannel bigint(20)    COMMENT '注册子渠道' ,
    reg_channel_name string    COMMENT '注册渠道名称' ,
    reg_subchannel_name string    COMMENT '注册子渠道名称' ,
    user_type_cw string    COMMENT '用户类型' 
)  COMMENT = '每日当季度累计资产用户数';
"""


finance_assets_futu_au_assets_daily_df_sql="""
insert overwrite table dws.finance_assets_futu_au_assets_daily_df partition(pt_date) --日期+用户数 --insert overwrite是删除原有数据然后新增数据，如果有分区那么只会删除指定分区数据，其他分区数据不受影响。本质是覆盖数据
select *
from(
    select  total_assets_g0_user,
            total_assets_g0_user_new,
            total_assets_g0_user_back,
            total_assets_g0_user_loss,
            total_assets_g0_user_keep,
            (total_assets_g0_user - lag(total_assets_g0_user,1,0) over(order by pt_date)) total_assets_g0_user_net,  --窗口函数
            pt_date,
            kpi_channel,
            kpi_subchannel,
            reg_channel,
            reg_subchannel,
            reg_channel_name,
            reg_subchannel_name,
    from(
        select  count(if (total_assets>0,1,null)) total_assets_g0_user,
                count(if (assets_g0_lifecycle='new',1,null)) total_assets_g0_user_new,
                count(if (assets_g0_lifecycle='back',1,null)) total_assets_g0_user_back,
                count(if (assets_g0_lifecycle='loss',1,null)) total_assets_g0_user_loss,
                count(if (assets_g0_lifecycle='keep',1,null)) total_assets_g0_user_keep,
                pt_date,
                kpi_channel,
                kpi_subchannel,
                reg_channel,
                reg_subchannel,
                reg_channel_name,
                reg_subchannel_name,             
        from(
            select  c1.*,
                    c2.kpi_channel,
                    c2.kpi_subchannel,
                    c2.reg_channel,
                    c2.reg_subchannel,
                    c2.reg_channel_name,
                    c2.reg_subchannel_name,                 
            from   dws.finance_assets_futu_au_user_assets_detail_daily c1     --日期+资产+用户     
            left join  dim.public_public_futu_au_user_channel_fact_df c2 on c1.user_id=c2.user_id 
            where  pt_date between '{start_date}' and '{end_date}'
        )c
        group by pt_date,kpi_channel,kpi_subchannel,reg_channel,reg_channel_name,reg_subchannel_name
    )b
)a
where pt_date='{end_date}'
"""

# trunc('yyyy-MM-dd','MM')  :当月第一天
# last_day('yyyy-MM-dd','MM')  :当月最后一天
# trunc('yyyy-MM-dd','Q'): 当前季度第一天
finance_assets_futu_au_assets_indicator_statis_m2d_df_sql="""
insert overwrite table dws.finance_assets_futu_au_assets_indicator_statis_m2d_df partition(pt_date)
select  sum(total_assets_g0_user_new)   total_assets_g0_user_new,           --为什么还要sum一次
        sum(total_assets_g0_user_net)   total_assets_g0_user_net,
        sum(total_assets_g0_user_loss_m)    total_assets_g0_user_loss_m,
        sum(total_assets_g0_user_keep_m)    total_assets_g0_user_keep_m,
        sum(total_assets_g0_user_back_m)    total_assets_g0_user_back_m,
        '{pt_date}'                     pt_date
from(
    select  sum(total_assets_g0_user_new)   total_assets_g0_user_new,
            sum(total_assets_g0_user_net)   total_assets_g0_user_net,
            0   total_assets_g0_user_loss_m,
            0   total_assets_g0_user_keep_m,
            0   total_assets_g0_user_back_m
    from  dws.finance_assets_futu_au_assets_daily_df
    -- 当月第一天开始累计计算新增等
    where  pt_date > date_add(trunc('{pt_date}','MM'),-1) and pt_date <= '{pt_date}'
    union all
    select  0   total_assets_g0_user_new,
            0   total_assets_g0_user_net,
            count(if(total_assets<=0 and last_total_assets>0 ,1,null)) total_assets_g0_user_loss_m,
            count(if(total_assets>0 and last_total_assets>0 ,1,null)) total_assets_g0_user_keep_m,
            count(if(total_assets>0 and last_total_assets<=0 and first_total_assets_g0_date <= date_add(trunc('{pt_date}','MM'),-1),1,null)) total_assets_g0_user_back_m,  --回流的逻辑！
    from(
        -- 当日月流失有资产客户数 需要对比当天与上月最后一天的数据
        select  nvl(b1.user_id,b2.user_id)  user_id,
                nvl(b1.total_assets,0)  total_assets,
                nvl(b2.total_assets,0)  last_total_assets,
                nvl(b1.first_total_assets_g0_date,b2.first_total_assets_g0_date) first_total_assets_g0_date,
        from  (
            select  user_id,
                    sum(total_assets) total_assets,
                    min(first_total_assets_g0_date) first_total_assets_g0_date,
            from  dws.finance_assets_futu_au_user_assets_detail_daily
            where  pt_date ='{pt_date}'  --当天数据
            group by user_id
        ) b1
        full outer join (
            select  user_id,
                    sum(total_assets) total_assets,
                    min(first_total_assets_g0_date) first_total_assets_g0_date,
            from  dws.finance_assets_futu_au_user_assets_detail_daily
            where  pt_date = date_add(trunc('{pt_date}','MM'),-1)  --上个月最后一天
            group by user_id
        ) b2
        on  b1.user_id=b2.user_id
    )a
)t
"""


finance_assets_futu_au_assets_quarter_d_sql="""
insert overwrite table dws.finance_assets_futu_au_assets_quarter_d partition(pt_date)
select  sum(total_assets_g0_user_new)   total_assets_g0_user_new,
        sum(total_assets_g0_user_net)   total_assets_g0_user_net,
        sum(pay_user_new)               pay_user_new,
        sum(pay_user_net)               pay_user_net,
        sum(effect_user_new)            effect_user_new,
        sum(effect_user_net)            effect_user_net,
        sum(total_assets_g0_user_loss_q)    total_assets_g0_user_loss_q,
        sum(total_assets_g0_user_keep_q)    total_assets_g0_user_keep_q,
        sum(total_assets_g0_user_back_q)    total_assets_g0_user_back_q,
        sum(pay_user_loss_q)            pay_user_loss_q,
        sum(pay_user_keep_q)            pay_user_keep_q,
        sum(pay_user_back_q)            pay_user_back_q,
        sum(effect_user_loss_q)         effect_user_loss_q,
        sum(effect_user_keep_q)         effect_user_keep_q,
        sum(effect_user_back_q)         effect_user_back_q,
        '{pt_date}'                     pt_date
from(
    select  sum(total_assets_g0_user_new)   total_assets_g0_user_new,
            sum(total_assets_g0_user_net)   total_assets_g0_user_net,
            sum(pay_user_new)               pay_user_new,
            sum(pay_user_net)               pay_user_net,
            sum(effect_user_new)            effect_user_new,
            sum(effect_user_net)            effect_user_net,
            0   total_assets_g0_user_loss_q,
            0   total_assets_g0_user_keep_q,
            0   total_assets_g0_user_back_q,
            0   pay_user_loss_q,
            0   pay_user_keep_q,
            0   pay_user_back_q,
            0   effect_user_loss_q,
            0   effect_user_keep_q,
            0   effect_user_back_q
    from  dws.finance_assets_futu_au_assets_daily
    -- 当季第一天开始累计计算新增等
    where  pt_date > date_add(trunc('{pt_date}','Q'),-1) and pt_date <= '{pt_date}'
    union all
    select  0   total_assets_g0_user_new,
            0   total_assets_g0_user_net,
            0   pay_user_new,
            0   pay_user_net,
            0   effect_user_new,
            0   effect_user_net,
            count(if(total_assets<=0 and last_total_assets>0 ,1,null)) total_assets_g0_user_loss_q,
            count(if(total_assets>0 and last_total_assets>0 ,1,null)) total_assets_g0_user_keep_q,
            count(if(total_assets>0 and last_total_assets<=0 and first_total_assets_g0_date <= date_add(trunc('{pt_date}','Q'),-1),1,null)) total_assets_g0_user_back_q,
            count(if(total_assets<=0 and last_total_assets>0 and date(from_utc_timestamp(first_pay_time*1000,'Asia/Shanghai'))<=date_add(trunc('{pt_date}','Q'),-1),1,null )) pay_user_loss_q,
            count(if(total_assets>0 and last_total_assets>0 and date(from_utc_timestamp(first_pay_time*1000,'Asia/Shanghai'))<=date_add(trunc('{pt_date}','Q'),-1),1,null )) pay_user_keep_q,
            count(if(total_assets>0 and last_total_assets<=0 and date(from_utc_timestamp(first_pay_time*1000,'Asia/Shanghai'))<=date_add(trunc('{pt_date}','Q'),-1),1,null )) pay_user_back_q,
            count(if(total_assets<=1762 and last_total_assets>1762 ,1,null)) total_assets_gset_user_loss_q,
            count(if(total_assets>1762 and last_total_assets>1762 ,1,null)) total_assets_gset_user_keep_q,
            count(if(total_assets>1762 and last_total_assets<=1762 and first_total_assets_gset_date <= date_add(trunc('{pt_date}','Q'),-1),1,null)) total_assets_gset_user_back_q
    from(
        -- 当日月流失有资产客户数 需要对比当天与上季度最后一天的数据
        select  nvl(b1.user_id,b2.user_id)  user_id,
                nvl(b1.total_assets,0)  total_assets,
                nvl(b2.total_assets,0)  last_total_assets,
                nvl(b1.first_total_assets_g0_date,b2.first_total_assets_g0_date) first_total_assets_g0_date,
                nvl(b1.first_total_assets_gset_date,b2.first_total_assets_gset_date) first_total_assets_gset_date,
                b3.first_cash_stock_in_time first_pay_time
        from  (
            select  user_id,
                    sum(total_assets) total_assets,
                    min(first_total_assets_g0_date) first_total_assets_g0_date,
                    min(first_total_assets_gset_date) first_total_assets_gset_date
            from  dws.finance_assets_futu_au_user_assets_detail_daily
            where  pt_date ='{pt_date}'
            group by user_id
        ) b1
        full outer join (
            select  user_id,
                    sum(total_assets) total_assets,
                    min(first_total_assets_g0_date) first_total_assets_g0_date,
                    min(first_total_assets_gset_date) first_total_assets_gset_date
            from  dws.finance_assets_futu_au_user_assets_detail_daily
            where  pt_date = date_add(trunc('{pt_date}','Q'),-1)
            group by user_id
        ) b2
        on  b1.user_id=b2.user_id
        -- 关联入资记录表 判断付费用户
        left join dwd.finance_counter_futu_au_first_cash_stock_net_success_time b3
        on nvl(b1.user_id,b2.user_id)=b3.user_id
    )a
)t
"""

