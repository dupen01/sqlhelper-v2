from src.utils import read_from_file

# 读取目录或文件
sql_path = "/Users/dunett/codes/duperl/daas-migration/showyu_fastdata_backup_20250701/数字运营_经营看板任务5_业绩顾客任务/*.sql"
sql_stmt_str = read_from_file(sql_path)

# 读取文本
sql_stmt_str = """
-- 导入会员在各个系统的id
INSERT OVERWRITE dws_hive.dws_member_sys_relation
select distinct IFNULL(m.union_one_id, t2.oneid) union_one_id,
                t2.sys_id,
                t2.sys
from (
         select erp1.oneid, id1 sys_id, 'erp1' sys
         from dwd_hive.dwd_member_relation erp1
         union all
         select erp2.oneid, id2 sys_id, 'erp2' sys
         from dwd_hive.dwd_member_relation erp2
         union all
         -- 春语的会员id = erp2.id
         select t1.oneid, t1.id2 sys_id, 'chunyu' sys
         from ods_hive.ods_xiuyu_temp_customer c -- 春语会员表
                  inner join (
             select erp2.oneid, id2
             from dwd_hive.dwd_member_relation erp2
         ) t1 on t1.id2 = c.showyu_member_id
     ) t2
         left join dws_hive.dws_fact_customer_archives m on m.one_id = t2.oneid
where t2.sys_id is not null
;


-- 计算春语新客
INSERT OVERWRITE dws_hive.dws_chunyu_member_new_guest
select msr.union_one_id union_one_id,
       date_format(min(p.performance_belong_date), '%Y-%m-%d') new_guest_date
from ods_hive.ods_dw_cy_shop_performanceamountinout p
         inner join dws_hive.dws_member_sys_relation msr on msr.sys_id = p.customer_erpid and msr.sys = 'chunyu'
group by msr.union_one_id
;

-- 导入春语主题活动明细
INSERT OVERWRITE dws_hive.dws_chunyu_activities_detail
select
    p.showyu_org_code shop_code,
    p.showyu_org_full_name shop_name,
    m.serial_num member_code,
    m.nick_name member_name,
    '' activities_id,
    ip.promotion_item activities_name,
    concat(ip.month, '-01 00:00:00') start_date,
    concat(date(date_add(add_months(concat(ip.month, '-01'), 1), -1)), ' 23:59:59') as end_date,
    p.performance_order_number order_number,
    p.order_pay_full_time order_buy_date,
    if(p.business_type = 1, r.perform_money, 0 - r.perform_money) order_amount,
    p.business_type,
    if(g.union_one_id is not null, 1,0) new_guest_state
from (
     select
         showyu_org_code,
         showyu_org_full_name,
         performance_order_number,
         order_pay_full_time,
         business_type,
         showyu_member_id
     from ods_hive.ods_chunyu_order_store_performance_report
     where dr = 0
      and business_type in (1,4)
    group by showyu_org_code,
             showyu_org_full_name,
             performance_order_number,
             order_pay_full_time,
             business_type,
             showyu_member_id
         )  p
inner join (
    select billing_no,
           activity_code,
           settle_date,
           sum(perform_money) perform_money
    from ods_hive.ods_xiuyu_activity_manager_report
    group by billing_no,
             activity_code,
             settle_date
    ) r on p.performance_order_number = r.billing_no
inner join ods_hive.ods_important_promotion ip on ip.activity_code = r.activity_code and ip.dr = 0
                                                and ip.month = date_format(r.settle_date, '%Y-%m')
inner join dws_hive.dws_member_sys_relation msr on msr.sys_id = p.showyu_member_id and msr.sys = 'chunyu'
inner join ods_hive.ods_member_batch m on m.id = p.showyu_member_id
left join dws_hive.dws_chunyu_member_new_guest g on g.union_one_id = msr.union_one_id and g.new_guest_date = date_format(r.settle_date, '%Y-%m-%d')
;

"""


def test_get_all_source_tables():
    """获取SQL语句中所有来源表名"""
    from src.utils import get_all_source_tables

    source_tables = get_all_source_tables(sql_stmt_str)
    for table in source_tables:
        # 仅打印ods表
        # if table.startswith("ods_hive"):
        print(table)


def test_get_root_tables():
    """获取最原始来源表，ODS"""
    from src.utils import get_root_tables

    source_tables = get_root_tables(sql_stmt_str)
    for table in source_tables:
        # 仅打印dws表
        # if table.startswith("dws_hive"):
        print(table)


def test_get_leaf_tables():
    """获取最下游的表, 即没有下游任务的表，dws、ads等"""
    from src.utils import get_leaf_tables

    target_tables = get_leaf_tables(sql_stmt_str)
    for table in target_tables:
        print(table)


def test_print_mermaid_dag():
    """打印DAG图"""
    from src.utils import print_mermaid_dag

    print_mermaid_dag(sql_stmt_str)


def test_print_related_edges_upstream():
    from src.utils import print_related_edges_upstream

    print_related_edges_upstream(sql_stmt_str, "dws_hive.dws_chunyu_member_new_guest")


def test_get_related_first_source_tables_upstream():
    from src.utils import get_related_first_source_tables_upstream

    tables = get_related_first_source_tables_upstream(sql_stmt_str, "dws_hive.dws_chunyu_member_new_guest")
    for table in tables:
        print(table)


def test_print_related_edges_downstream():
    from src.utils import print_related_edges_downstream

    print_related_edges_downstream(sql_stmt_str, "dws_hive.dws_chunyu_member_new_guest")


def test_mermaidjs():
    from src.utils import visualize_dag

    visualize_dag(sql_stmt_str)


if __name__ == "__main__":
    # test_get_root_tables()
    # test_get_all_source_tables()
    # test_print_mermaid_dag()
    # test_get_leaf_tables()
    # test_save_as_html()
    # test_get_related_first_source_tables_upstream()
    # test_print_related_edges_upstream()
    # test_print_related_edges_downstream()
    test_mermaidjs()
    ...
