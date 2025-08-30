import glob
from pathlib import Path
from typing import List, Set, Tuple

from .graph import DagGraph
from .helper import SqlHelper


def read_from_file(file_path: str) -> str:
    """
    从文件或目录中读取SQL语句

    Args:
        file_path: 文件路径，支持通配符

    Returns:
        读取的SQL字符串
    """
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
        with open(sql_file, "r") as f:
            sql_str = f.read()
        if not sql_str.strip().endswith(";"):
            sql_str = sql_str + ";\n"
        sql_stmt_str += sql_str
    return sql_stmt_str


def get_all_source_tables(sql_stmt_str: str) -> List[str]:
    """
    获取所有SQL语句中涉及的源表，包含了中间表

    Args:
        sql_stmt_str: SQL语句字符串

    Returns:
        源表列表
    """
    sql_stmt_lst = SqlHelper.split(sql_stmt_str)
    source_tables = set()

    for sql_stmt in sql_stmt_lst:
        table_info = SqlHelper().get_source_target_tables(sql_stmt)
        if table_info:
            source_tables.update(table_info["source_table"])

    return list(source_tables)


def pretty_print_lineage(sql_stmt_str: str) -> None:
    sql_stmt_lst = SqlHelper.split(sql_stmt_str)
    result = {}
    for sql_stmt in sql_stmt_lst:
        table_info = SqlHelper().get_source_target_tables(sql_stmt)
        # print(table_info)
        if table_info:
            source_tables = table_info.get("source_table", [])
            target_tables = table_info.get("target_table", [])

            # 为每个目标表添加源表列表
            for target_table in target_tables:
                target_table_clean = target_table.replace("`", "")
                if target_table_clean not in result:
                    result[target_table_clean] = []
                # 添加源表（去重但保持顺序）
                for source_table in source_tables:
                    source_table_clean = source_table.replace("`", "")
                    if source_table_clean not in result[target_table_clean]:
                        result[target_table_clean].append(source_table_clean)

    for target_table_clean, source_tables in result.items():
        print(target_table_clean)
        for source_table in source_tables:
            print(f"  ├─ {source_table}")
    return


def print_mermaid_dag(sql_stmt_str: str) -> None:
    """
    打印SQL语句的DAG图（mermaid格式）

    Args:
        sql_stmt_str: SQL语句字符串
    """
    sql_stmt_lst = SqlHelper.split(sql_stmt_str)
    dg = _sql_to_dag(sql_stmt_lst)
    dg.print_all_edges_to_mermaid()


def _sql_to_dag(sql_stmt_lst: List[str]) -> DagGraph:
    """
    将SQL语句列表转换为DAG图

    Args:
        sql_stmt_lst: SQL语句列表

    Returns:
        DAG图对象
    """
    dg = DagGraph()
    for sql_stmt in sql_stmt_lst:
        table_info = SqlHelper().get_source_target_tables(sql_stmt)
        if table_info:
            target_tables = table_info["target_table"]
            source_tables = table_info["source_table"]
            for source_table in source_tables:
                if target_tables:
                    for target_table in target_tables:
                        dg.add_edge(source_table.replace("`", ""), target_table.replace("`", ""))
    return dg


def _collect_tables(sql_stmt_str: str) -> Tuple[Set[str], Set[str]]:
    """
    收集SQL语句中的源表和目标表

    Args:
        sql_stmt_str: SQL语句字符串

    Returns:
        (源表集合, 目标表集合)
    """
    sql_stmt_lst = SqlHelper.split(sql_stmt_str)
    source_tables = set()
    target_tables = set()

    for sql_stmt in sql_stmt_lst:
        table_info = SqlHelper().get_source_target_tables(sql_stmt)
        if table_info:
            # 处理源表
            for source_table in table_info["source_table"]:
                source_tables.add(source_table.replace("`", ""))
            # 处理目标表
            for target_table in table_info["target_table"]:
                target_tables.add(target_table.replace("`", ""))

    return source_tables, target_tables


def get_root_tables(sql_stmt_str: str) -> List[str]:
    """
    获取没有上游写入的底表，比如ods表，没有写入任务的表

    Args:
        sql_stmt_str: SQL语句字符串

    Returns:
        根表列表
    """
    source_tables, target_tables = _collect_tables(sql_stmt_str)
    return list(source_tables - target_tables)


def get_leaf_tables(sql_stmt_str: str) -> List[str]:
    """
    获取没有下游任务的目标表，比如ads表，最下游的表

    Args:
        sql_stmt_str: SQL语句字符串

    Returns:
        叶子表列表
    """
    source_tables, target_tables = _collect_tables(sql_stmt_str)
    return list(target_tables - source_tables)


def print_related_edges_upstream(sql_stmt_str: str, target_table: str) -> None:
    """
    向前查找与目标表相关的表，并打印出所有相关表的DAG

    Args:
        sql_stmt_str: SQL语句字符串
        target_table: 目标表名
    """
    sql_stmt_lst = SqlHelper.split(sql_stmt_str)
    dg = _sql_to_dag(sql_stmt_lst)
    related_edges = dg.find_related_edges_upstream(target_table)
    dg.print_edges_to_mermaid(related_edges)


def get_related_first_source_tables_upstream(sql_stmt_str: str, target_table: str) -> List[str]:
    """
    查询与目标表相关的表，返回第一层原始来源表

    Args:
        sql_stmt_str: SQL语句字符串
        target_table: 目标表名

    Returns:
        第一层原始来源表列表
    """
    # related_edges = _get_related_edges_forward(sql_stmt_str, target_table)
    sql_stmt_lst = SqlHelper.split(sql_stmt_str)
    dg = _sql_to_dag(sql_stmt_lst)
    related_edges = dg.find_related_edges_upstream(target_table)
    if not related_edges:
        return []

    source_tables = set(edge[0] for edge in related_edges)
    target_tables = set(edge[1] for edge in related_edges)
    return list(source_tables - target_tables)


def print_related_edges_downstream(sql_stmt_str: str, target_table: str) -> None:
    """
    向前查找与目标表相关的表，并打印出所有相关表的DAG

    Args:
        sql_stmt_str: SQL语句字符串
        target_table: 目标表名
    """
    sql_stmt_lst = SqlHelper.split(sql_stmt_str)
    dg = _sql_to_dag(sql_stmt_lst)
    related_edges = dg.find_related_edges_downstream(target_table)
    dg.print_edges_to_mermaid(related_edges)


def visualize_dag(sql_stmt_str: str, filename: str = "dag_mermaid.html", title: str = "DAG Visualization") -> None:
    """
    可视化DAG图

    Args:
        sql_stmt_str: SQL语句字符串
    """
    import os
    import webbrowser

    sql_stmt_lst = SqlHelper.split(sql_stmt_str)
    dg = _sql_to_dag(sql_stmt_lst)

    html_content = dg.get_mermaidjs_dag(title)

    with open(filename, "w", encoding="utf-8") as f:
        f.write(html_content)

    # 获取绝对路径并打开
    abs_path = os.path.abspath(filename)
    webbrowser.open(f"file://{abs_path}")
    print(f"Mermaid.js HTML文件已生成并打开: {abs_path}")
