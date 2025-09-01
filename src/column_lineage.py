from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass

import sqlglot
from sqlglot import expressions as exp
from sqlglot.optimizer.qualify import qualify


@dataclass
class UnionContext:
    """
    UnionContext 用于记录并管理 SQL 中 UNION 的上下文信息。
    """

    in_union = False
    is_union_main_query = False
    union_main_alias = []
    output_cols_length = 0


class ColumnLineageExtractor:
    def __init__(self, sql, dialect=None):
        """
        初始化字段血缘提取器
        :param sql: SQL 语句
        :param dialect: SQL 方言（可选）
        """
        self.sql = sql
        self.dialect = dialect
        self.scope_stack = []  # 作用域栈
        self.target_table = "unknown"
        self.target_columns = []
        self.visited = set()

        self.column_mapping = []
        self.column_lineage = []

        # union 上下文
        self.union_context = defaultdict(UnionContext)


    def extract(self):
        """
        主入口：解析 SQL 并提取字段血缘
        """
        try:
            self.ast = sqlglot.parse_one(self.sql, read=self.dialect)
            # 执行表别名限定（自动添加缺失的表别名）
            self.ast = qualify(self.ast)
            print(f"优化后的SQL: {self.ast}")

            # 提取血缘关系
            self._traverse_ast(self.ast)

            self._resolve_column_lineage()
            self._finalize_lineage()

            return {
                "column_lineage": self.column_lineage,
            }

        except Exception as e:
            raise ValueError(f"SQL 解析失败: {str(e)}")

    def _traverse_ast(self, node: exp.Expression):
        """递归遍历 AST 并提取血缘关系"""
        # 避免重复访问节点
        if node in self.visited:
            return

        # 根据节点类型处理
        if isinstance(node, (exp.Insert, exp.Create)):
            self._handle_insert_node(node)
        elif isinstance(node, exp.Select):
            self._handle_select_node(node)
        elif isinstance(node, exp.With):
            self._handle_with_node(node)
        elif isinstance(node, exp.Subquery):
            self._handle_subquery_node(node)

        # 递归处理子节点
        self._traverse_children(node)
        self.visited.add(node)

    def _handle_insert_node(self, node: exp.Insert | exp.Create):
        """处理 INSERT 节点"""
        if isinstance(node.this, exp.Table):
            self.target_table = self._get_full_table_name(node.this)
        elif isinstance(node.this, exp.Schema):
            schema = node.this
            self.target_table = self._get_full_table_name(schema.this)
            self.target_columns = [col.this for col in schema.expressions]

    def _handle_select_node(self, node: exp.Select):
        """处理 SELECT 节点"""
        table_map = {}

        # 注册主表
        from_node = node.args.get("from") or exp.From()
        self._register_table(from_node.this, table_map)

        # 注册连接表
        joins = node.args.get("joins") or []
        for join in joins:
            self._register_table(join.this, table_map)

        # 处理 union 查询
        self.union_context[self.current_scope].in_union = False
        self.union_context[self.current_scope].is_union_main_query = False

        if isinstance(node.parent, exp.Union):
            self.union_context[self.current_scope].in_union = True
            if node.arg_key == "this":
                self.union_context[self.current_scope].is_union_main_query = True
            if self.target_columns:
                self.union_context["main"].output_cols_length = len(self.target_columns)

        # 处理每一个字段
        for i, select_expr in enumerate(node.selects):
            alias = select_expr.alias_or_name

            if self.union_context[self.current_scope].is_union_main_query:
                self.union_context[self.current_scope].union_main_alias.append(alias)

            if self.union_context[self.current_scope].in_union and not self.union_context[self.current_scope].is_union_main_query:
                alias = self.union_context[self.current_scope].union_main_alias[i]

            output = f"{self.current_scope}.{alias}"

            columns = list(select_expr.find_all(exp.Column))
            if columns:
                self._handle_column_expressions(columns, table_map, output)
            else:
                self._handle_non_column_expressions(select_expr, table_map, output)

    def _handle_column_expressions(self, columns, table_map, output):
        """处理列表达式"""
        _type = "column"
        for column in columns:
            real_table = table_map.get(column.table, column.table)
            input = f"{real_table}.{column.name}"

            # if isinstance(column.this, exp.Star):
            # if column.is_star:
            #     output = f"{self.current_scope}.*"
            #     _type = "star"
            # else:
            #     _type = "column"

            self.column_mapping.append({"input": input, "output": output, "type": _type})

    def _handle_non_column_expressions(self, select_expr, table_map, output):
        """处理非字段表达式（如函数、字面量等）"""
        # 处理 select *
        if select_expr.is_star:
            self._handle_star_expression(table_map)
        else:
            select_node = select_expr.this
            select_node.pop_comments()
            select_sql = select_node.sql()

            if isinstance(select_node, exp.Func):
                _type = "function"
            elif isinstance(select_node, exp.Literal):
                _type = "literal"
            else:
                _type = "expression"

            self.column_mapping.append({"input": select_sql, "output": output, "type": _type})

    def _handle_star_expression(self, table_map):
        """处理星号表达式"""
        _type = "star"
        output = f"{self.current_scope}.*"

        # if self.union_context[self.current_scope]['in_union'] and not self.union_context[self.current_scope]['is_union_main_query']:
        #     for col in self.union_context[self.current_scope]['union_main_alias']:
        #         item = {"input": 'unkown', "output": col, "type": _type}
        #         self.column_mapping.append(item)

        for table in table_map.values():
            input = f"{table}.*"
            # print("_handle_star_expression: ", {"input": input, "output": output, "type": _type})
            self.column_mapping.append({"input": input, "output": output, "type": _type})

    def _handle_with_node(self, node: exp.With):
        """处理 WITH 子句（CTE）"""
        for cte in node.expressions:
            cte_name = cte.alias
            with self._scope_context(cte_name):
                self._traverse_ast(cte)

    def _handle_subquery_node(self, node: exp.Subquery):
        """处理子查询"""
        subquery_alias = node.alias
        with self._scope_context(subquery_alias):
            self._traverse_ast(node.this)

    def _traverse_children(self, node: exp.Expression):
        """遍历节点的子节点"""
        for child in node.args.values():
            if isinstance(child, (list, tuple)):
                for item in child:
                    if isinstance(item, exp.Expression):
                        self._traverse_ast(item)
            elif isinstance(child, exp.Expression):
                self._traverse_ast(child)

    @contextmanager
    def _scope_context(self, scope_name):
        """作用域上下文管理器"""
        self._enter_scope(scope_name)
        try:
            yield
        finally:
            self._exit_scope()

    def _enter_scope(self, scope_name):
        """进入新的作用域"""
        self.scope_stack.append(scope_name)

    def _exit_scope(self):
        """退出当前作用域"""
        if self.scope_stack:
            self.scope_stack.pop()

    @property
    def current_scope(self):
        """获取当前作用域"""
        return self.scope_stack[-1] if self.scope_stack else "main"

    def _get_full_table_name(self, table_node: exp.Table):
        table_name = table_node.name
        if table_node.db:
            table_name = f"{table_node.db}.{table_name}"
        if table_node.catalog:
            table_name = f"{table_node.catalog}.{table_name}"

        return table_name

    def _register_table(self, table, table_map: dict):
        """注册表到表映射中"""
        if isinstance(table, exp.Table):
            table_name = self._get_full_table_name(table)
            alias = table.alias or table_name

            table_map[alias] = table_name

    def _find_real_column(self, items: list[dict], parent_id):
        """获取main作用域下的字段的源表和源字段"""
        children_map = defaultdict(list)
        for item in items:
            children_map[item["output"]].append(item)

        def _get_leaf_node(parent_id):
            leaf_nodes = set()
            for item in children_map[parent_id]:
                if not children_map[item["input"]]:
                    leaf_nodes.add((item["input"], item["type"]))
                else:
                    leaf_nodes.update(_get_leaf_node(item["input"]))
            return leaf_nodes or {parent_id}

        return _get_leaf_node(parent_id)

    def _resolve_column_lineage(self):
        """解析字段血缘关系"""
        self.column_lineage = []
        for item in self.column_mapping:
            output_scope = item["output"].split(".")[0]
            if output_scope == "main":
                real_columns = self._find_real_column(self.column_mapping, item["output"])
                output_column = item["output"].split(".")[-1]

                lineage_item = {"column": output_column, "original_columns": list(real_columns)}

                if lineage_item not in self.column_lineage:
                    self.column_lineage.append(lineage_item)

    def _finalize_lineage(self):
        """最终处理字段血缘关系"""
        new_column_lineage = []
        # try:
        #     # if self.union_context["main"]["union_main_alias"] and not self.target_columns:
        #     if self.union_context["main"].union_main_alias and not self.target_columns:
        #         self.target_columns = self.union_context["main"].union_main_alias
        # except KeyError:
        #     pass

        for i, item in enumerate(self.column_lineage):
            new_original_columns = self._process_original_columns(item["original_columns"])
            new_output_column = self._get_output_column(i, item["column"])
            if new_output_column:
                new_item = {"column": new_output_column, "original_columns": new_original_columns}
                new_column_lineage.append(new_item)

        self.column_lineage = new_column_lineage

    def _process_original_columns(self, original_columns: list[tuple[str, str]]):
        """
        处理原始列信息
        """

        has_column = any(item[1] == "column" for item in original_columns)

        if has_column:
            return [item[0] for item in original_columns if item[1] == "column"]
        else:
            return [item[0] for item in original_columns]

    def _get_output_column(self, index, default_column):
        """获取输出列名"""

        try:
            if self.target_columns:
                return self.target_columns[index]
            else:
                return default_column
        except IndexError:
            return None

    def _process_lineage_data(self, lineage_data):
        """
        处理血缘数据，提取公共部分供显示方法使用
        """

        # 检查输入数据
        if not lineage_data:
            return None, {}

        # 获取输出表名
        output_table = lineage_data.get("output_tables", "unknown")
        if isinstance(output_table, list) and output_table:
            output_table = output_table[0]
        elif not isinstance(output_table, str):
            output_table = str(output_table) if output_table else "unknown"

        # 使用字典按目标字段分组
        lineage_dict = defaultdict(lambda: {"source_tables": set(), "source_fields": []})

        # 安全获取column_lineage
        column_lineage = lineage_data.get("column_lineage", [])

        for column_info in column_lineage:
            if not isinstance(column_info, dict):
                continue

            target_column = column_info.get("column", "unknown")
            if not target_column:
                continue

            original_columns = column_info.get("original_columns", [])
            for orig_item in original_columns:
                try:
                    # 处理原始字段，确保正确解包
                    if isinstance(orig_item, tuple) and len(orig_item) >= 2:
                        orig_col, _type = orig_item[0], orig_item[1]
                    elif isinstance(orig_item, str):
                        orig_col = orig_item
                    else:
                        orig_col = str(orig_item) if orig_item else ""

                    # 解析原始字段的表和列名
                    if orig_col and "." in orig_col:
                        parts = orig_col.rsplit(".", 1)
                        if len(parts) == 2:
                            source_table, source_column = parts
                            lineage_dict[target_column]["source_tables"].add(source_table)
                    lineage_dict[target_column]["source_fields"].append(orig_col if orig_col else "")
                except Exception:
                    continue

        # 处理字段显示格式
        processed_lineage = {}
        for target_column, sources in lineage_dict.items():
            try:
                source_tables_str = ", ".join(sorted(sources["source_tables"])) if sources["source_tables"] else ""

                # 如果只有一个来源表，则只显示字段名，否则显示完整格式
                if len(sources["source_tables"]) == 1 and sources["source_tables"]:
                    # 只有一个来源表时，仅显示字段名部分
                    source_fields = []
                    for field in sources["source_fields"]:
                        if field and "." in field:
                            source_fields.append(field.split(".")[-1])  # 只取字段名部分
                        elif field:
                            source_fields.append(field)
                    source_fields_str = ", ".join(source_fields) if source_fields else ""
                else:
                    # 多个来源表时，显示完整格式
                    source_fields_str = ", ".join(sources["source_fields"]) if sources["source_fields"] else ""

                processed_lineage[target_column] = {
                    "source_tables_str": source_tables_str,
                    "source_fields_str": source_fields_str,
                }
            except Exception:
                continue

        return output_table, processed_lineage

    def display_compact_rich_table(self, lineage_data):
        try:
            from rich.console import Console
            from rich.table import Table
        except ImportError:
            print("错误: 未安装 rich 库，请运行 'pip install rich' 安装")
            return

        try:
            # 处理血缘数据
            output_table, processed_lineage = self._process_lineage_data(lineage_data)

            if output_table is None or not processed_lineage:
                print("警告: 未提供有效的血缘数据")
                return

            console = Console()

            # 创建表格
            table = Table(show_header=True, header_style="bold bright_green", show_lines=True)
            table.add_column("目标表", style="")
            table.add_column("目标字段", style="")
            table.add_column("来源表", style="")
            table.add_column("来源字段", style="")

            # 添加行
            for target_column, data in processed_lineage.items():
                table.add_row(
                    str(output_table) if output_table else "unknown",
                    str(target_column) if target_column else "unknown",
                    data["source_tables_str"],
                    data["source_fields_str"],
                )

            # 打印表格
            console.print(table)

        except Exception as e:
            print(f"显示血缘关系表时出错: {e}")

    def display_compact_html_table(self, lineage_data):
        try:
            import os
            import webbrowser
        except ImportError as e:
            print(f"错误: 缺少必要的库 {e}")
            return

        try:
            # 处理血缘数据
            output_table, processed_lineage = self._process_lineage_data(lineage_data)

            if output_table is None or not processed_lineage:
                print("警告: 未提供有效的血缘数据")
                return

            # 创建HTML表格
            html = """<!DOCTYPE html>
            <html lang="zh-CN">
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            font-size: 14px;
            line-height: 1.6;
            color: #333;
        }

        table {
            border-collapse: collapse;
            width: 100%;
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            font-size: 14px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        }

        th, td {
            border: 1px solid #ddd;
            padding: 12px 15px;
            text-align: left;
        }

        th {
            background-color: #f8f9fa;
            font-weight: 600;
            color: #212529;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            font-size: 13px;
        }

        td {
            color: #495057;
        }

        tr:nth-child(even) {
            background-color: #f8f9fa;
        }

        tr:hover {
            background-color: #e9ecef;
        }

        h2 {
            font-weight: 500;
            color: #212529;
            margin-bottom: 20px;
        }
        </style>
            </head>
            <body>
                <h2 style="text-align: center;">字段级血缘关系</h2>
                <table>
                    <tr>
                        <th>目标表</th>
                        <th>目标字段</th>
                        <th>来源表</th>
                        <th>来源字段</th>
                    </tr>
            """

            # 添加行
            for target_column, data in processed_lineage.items():
                html += f"""
                <tr>
                    <td>{output_table}</td>
                    <td>{target_column}</td>
                    <td>{data["source_tables_str"]}</td>
                    <td>{data["source_fields_str"]}</td>
                </tr>
                """

            # 结束HTML
            html += """
                </table>
            </body>
            </html>
            """

            # 保存到文件
            with open("compact_lineage_table.html", "w", encoding="utf-8") as f:
                f.write(html)

            abs_path = os.path.abspath("compact_lineage_table.html")
            webbrowser.open(f"file://{abs_path}")
            print(f"HTML文件已生成并打开: {abs_path}")

        except Exception as e:
            print(f"生成HTML血缘关系表时出错: {e}")
