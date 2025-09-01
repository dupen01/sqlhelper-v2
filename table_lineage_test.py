from src.utils import read_from_file

# 读取目录或文件
sql_path = "/Users/dunett/codes/duperl/daas-migration/showyu_fastdata_backup_20250701/数字运营_经营看板任务5_业绩顾客任务/*.sql"
sql_stmt_str = read_from_file(sql_path)

# 读取文本
sql_stmt_str = """
insert into tbl_a
(aid, aname, bid, bname, b100, etl_time)
with cte_a as (
select 
 a1 as id , 
 a2 as name 
from tbl_b
)
, cte_b as (
select 
 b1 as id , 
 `b2`*100 as  b100
from tbl_c
)
select
 a.id, 
 if( a.name is null, b.name, c.id),
 b.id as b_id,
 b.name as b_name,
 c.b100 as new_b100, 
 current_date() -- current_date() 
from cte_a as b
join (
 select 
   ifnull(t1.id, a.id) as id , 
   a.name 
 from tbl_c t1
 join aaa a on 1=1
) a
on a.id = b.id
left join cte_b as c on a.id = c.id
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


def test_pretty_print_lineage():
    """打印血缘关系"""
    from src.utils import pretty_print_lineage

    pretty_print_lineage(sql_stmt_str)


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


def test_mermaid_html():
    from src.utils import visualize_dag

    visualize_dag(sql_stmt_str)


if __name__ == "__main__":
    # test_get_root_tables()
    # test_get_leaf_tables()
    test_pretty_print_lineage()
    # test_get_all_source_tables()
    # test_print_mermaid_dag()
    # test_get_leaf_tables()
    # test_save_as_html()
    # test_get_related_first_source_tables_upstream()
    # test_print_related_edges_upstream()
    # test_print_related_edges_downstream()
    # test_mermaid_html()
    ...
