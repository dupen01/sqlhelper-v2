from src.utils import read_from_file

# 读取目录或文件
sql_path = "/Users/dunett/codes/duperl/daas-migration/showyu_fastdata_backup_20250701/数字运营_经营看板任务5_业绩顾客任务/*.sql"
sql_stmt_str = read_from_file(sql_path)

# 读取文本
# sql_stmt_str = """
# """


def test_get_all_related_tables_forward():
    """获取指定表的所有关联表"""
    from src.utils import get_all_related_tables_forward

    all_related_tables = get_all_related_tables_forward(sql_stmt_str, "dws_hive.dws_business_manager_performance_info")
    for x in all_related_tables:
        print(x)


def test_get_source_tables():
    """获取SQL语句中所有来源表名"""
    from src.utils import get_source_tables

    source_tables = get_source_tables(sql_stmt_str)
    for table in source_tables:
        # 仅打印ods表
        # if table.startswith("ods_hive"):
        print(table)


def test_get_first_level_source_tables():
    """获取最原始来源表"""
    from src.utils import get_first_level_source_tables

    target_tables = get_first_level_source_tables(sql_stmt_str)
    for table in target_tables:
        # 仅打印dws表
        # if table.startswith("dws_hive"):
        print(table)


def test_get_last_level_target_tables():
    """获取最下游的表, 即没有下游任务的表"""
    from src.utils import get_last_level_target_tables

    target_tables = get_last_level_target_tables(sql_stmt_str)
    for table in target_tables:
        print(table)


if __name__ == "__main__":
    test_get_last_level_target_tables()
    ...
