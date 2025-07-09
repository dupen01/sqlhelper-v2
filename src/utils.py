import glob
from pathlib import Path

from .graph import DagGraph
from .helper import SqlHelper


def read_from_file(file_path: str) -> str:
    sql_file_path = Path(file_path)
    sql_stmt_str = ""
    file_lst = []
    if sql_file_path.is_file():
        with open(sql_file_path, "r") as f:
            return f.read()

    elif sql_file_path.is_dir():
        file_lst = list(sql_file_path.iterdir())

    elif any(char in file_path for char in ["*", "?", "["]):
        # glob模式处理
        file_lst = glob.glob(file_path)

    for sql_file in file_lst:
        sql_str = open(sql_file, "r").read()
        if not sql_str.strip().endswith(";\n"):
            sql_str = sql_str + "\n;\n"
        sql_stmt_str += sql_str
    return sql_stmt_str


# def read_sql_str_to_sql_stmt_lst(sql_stmt_str: str) -> list:
#     return SqlHelper.split(sql_stmt_str)


def get_source_tables(sql_stmt_str: str) -> list:
    sql_stmt_lst = SqlHelper.split(sql_stmt_str)
    rs = []
    for sql_stmt in sql_stmt_lst:
        x = SqlHelper().get_source_target_tables(sql_stmt)
        if x:
            source_tables = x["source_table"]
            for source_table in source_tables:
                rs.append(source_table)
    return list(set(rs))


def print_dag(sql_stmt_str: str) -> None:
    sql_stmt_lst = SqlHelper.split(sql_stmt_str)
    dg = __sql_to_dag(sql_stmt_lst)
    dg.print_all_edges_to_mermaid()


def get_mermaid_str(sql_stmt_str: str) -> str:
    sql_stmt_lst = SqlHelper.split(sql_stmt_str)
    dg = __sql_to_dag(sql_stmt_lst)
    return dg.get_all_edges_to_mermaid()


def __sql_to_dag(sql_stmt_lst: list) -> DagGraph:
    dg = DagGraph()
    for sql_stmt in sql_stmt_lst:
        x = SqlHelper().get_source_target_tables(sql_stmt)
        if x:
            target_tables = x["target_table"]
            source_tables = x["source_table"]
            for source_table in source_tables:
                if target_tables:
                    for target_table in target_tables:
                        dg.add_edge(source_table.replace("`", ""), target_table.replace("`", ""))
    return dg


def get_first_level_source_tables(sql_stmt_str: str):
    """获取没有上游写入的底表，比如ods表，没有写入任务的表"""
    sql_stmt_lst = SqlHelper.split(sql_stmt_str)
    source_tables2 = []
    target_tables2 = []
    for sql_stmt in sql_stmt_lst:
        x = SqlHelper().get_source_target_tables(sql_stmt)
        if x:
            source_tables = x["source_table"]
            target_tables = x["target_table"]
            for source_table in source_tables:
                source_tables2.append(source_table.replace("`", ""))
            for target_table in target_tables:
                target_tables2.append(target_table.replace("`", ""))
    return [source_table for source_table in set(source_tables2) if source_table not in target_tables2]


def get_last_level_target_tables(sql_stmt_str: str):
    """获取没有下游任务的目标表，比如ads表，最下游的表"""
    sql_stmt_lst = SqlHelper.split(sql_stmt_str)
    source_tables2 = []
    target_tables2 = []
    for sql_stmt in sql_stmt_lst:
        x = SqlHelper().get_source_target_tables(sql_stmt)
        if x:
            source_tables = x["source_table"]
            target_tables = x["target_table"]
            for source_table in source_tables:
                source_tables2.append(source_table.replace("`", ""))
            for target_table in target_tables:
                target_tables2.append(target_table.replace("`", ""))
    return [target_tables for target_tables in set(target_tables2) if target_tables not in source_tables2]


def __get_related_edges_forward(sql_stmt_str: str, target_table: str):
    """向前查找与目标表相关的表，返回edge列表"""
    sql_stmt_lst = SqlHelper.split(sql_stmt_str)
    dg = __sql_to_dag(sql_stmt_lst)
    related_edges = dg.find_related_edges_forward(target_table)
    return related_edges


def print_related_edges_forward(sql_stmt_str: str, target_table: str):
    """向前查找与目标表相关的表，并打印出所有相关表的DAG"""
    related_edges = __get_related_edges_forward(sql_stmt_str, target_table)
    DagGraph().print_dag_from_edges(related_edges)


def get_related_first_source_tables_forward(sql_stmt_str: str, target_table: str):
    """查询与目标表相关的表，返回第一层原始来源表"""
    related_edges = __get_related_edges_forward(sql_stmt_str, target_table)
    source_tables = list(set([edge[0] for edge in related_edges]))
    target_tables = list(set([edge[1] for edge in related_edges]))
    return list(set([source_table for source_table in source_tables if source_table not in target_tables]))


def get_all_related_tables_forward(sql_stmt_str: str, target_table: str):
    """查询与目标表相关的表，返回所有相关表"""
    related_edges = __get_related_edges_forward(sql_stmt_str, target_table)
    source_tables = [edge[0] for edge in related_edges]
    target_tables = [edge[1] for edge in related_edges]
    return list(set(source_tables) | set(target_tables))
