from collections import defaultdict
from pprint import pprint

import sqlglot
from sqlglot import expressions as exp
from sqlglot.optimizer.qualify import qualify


class ColumnLineageExtractor:
    def __init__(self, sql, dialect=None):
        """
        初始化字段血缘提取器
        :param sql: SQL 语句
        :param dialect: SQL 方言（可选）
        """
        self.sql = sql
        self.dialect = dialect
        self._column_lineage = defaultdict(list)
        self.scope_stack = []  # 作用域栈
        self.target_table = "unknown"
        self.target_columns = []
        self.visited = set()

        self.column_mapping = []
        self.column_lineage = []

        self.input_tables = []

    def extract(self):
        """
        主入口：解析 SQL 并提取字段血缘
        """
        try:
            self.ast = sqlglot.parse_one(self.sql, read=self.dialect)
            # 执行表别名限定（自动添加缺失的表别名）
            self.ast = qualify(self.ast)
            # 提取血缘关系
            self._traverse_ast(self.ast)

            self._column_lineage = self._get_main_real_column()
            self.column_lineage = self._handle_column_lineage()
            lineage = {
                "input_tables": list(set(self.input_tables)),
                "output_tables": self.target_table,
                "column_lineage": self.column_lineage,
            }

            return lineage

        except Exception as e:
            raise ValueError(f"SQL 解析失败: {str(e)}")

    def _traverse_ast(self, node: exp.Expression):
        """递归遍历 AST 并提取血缘关系"""

        if isinstance(node, (exp.Insert, exp.Create)):
            # todo 去掉 table 的引号
            if isinstance(node.this, exp.Table):
                self.target_table = node.this.name
            elif isinstance(node.this, exp.Schema):
                schema = node.this
                self.target_table = schema.this.name
                self.target_columns = [col.this for col in schema.expressions]

        elif isinstance(node, exp.Select):
            table_map = {}

            from_node = node.args.get("from") or exp.From()
            self._register_table(from_node.this, table_map)

            joins = node.args.get("joins") or []
            for join in joins:
                self._register_table(join.this, table_map)

            for select_expr in node.selects:
                alias = select_expr.alias
                output = f"{self.current_scope}.{alias}"

                columns = list(select_expr.find_all(exp.Column))
                if columns:
                    for column in columns:
                        real_table = table_map.get(column.table, column.table)
                        full_column_name = f"{real_table}.{column.name}"
                        if isinstance(column.this, exp.Star):
                            output = f"{self.current_scope}.*"
                            _type = "star"
                        else:
                            _type = "column"
                        self.column_mapping.append({"input": full_column_name, "output": output, "type": _type})
                else:
                    # 处理 select *
                    if isinstance(select_expr, exp.Star):
                        _type = "star"
                        output = f"{self.current_scope}.*"
                        for table in table_map.values():
                            select_sql = f"{table}.*"
                            self.column_mapping.append({"input": select_sql, "output": output, "type": _type})
                    else:
                        select_node = select_expr.this
                        select_sql = select_node.sql()
                        if isinstance(select_node, exp.Func):
                            _type = "function"
                        elif isinstance(select_node, exp.Literal):
                            _type = "literal"
                        self.column_mapping.append({"input": select_sql, "output": output, "type": _type})

        # 1. 处理 WITH 子句（CTE）
        elif isinstance(node, exp.With):
            for cte in node.expressions:
                cte_name = cte.alias
                self._enter_scope(cte_name)
                self._traverse_ast(cte)
                self._exit_scope()
            self.visited.add(node)

        # 2. 处理子查询
        elif isinstance(node, exp.Subquery):
            subquery_alias = node.alias
            self._enter_scope(subquery_alias)
            self._traverse_ast(node.this)
            self._exit_scope()
            self.visited.add(node)

        # 5. 递归处理子节点
        for child in node.args.values():
            if node in self.visited:
                continue
            if isinstance(child, (list, tuple)):
                for item in child:
                    if isinstance(item, exp.Expression):
                        self._traverse_ast(item)
            if isinstance(child, exp.Expression):
                self._traverse_ast(child)

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

    def _register_table(self, table, table_map: dict):
        if isinstance(table, exp.Table):
            table_name = table.name
            alias = table.alias or table_name

            if table.db:
                table_name = f"{table.db}.{table.name}"
            if table.catalog:
                table_name = f"{table.catalog}.{table_name}"
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

    def _get_main_real_column(self):
        for item in self.column_mapping:
            output = item["output"]
            if output.split(".")[0] == "main":
                real_columns = self._find_real_column(self.column_mapping, output)

                output_column = output.split(".")[-1]

                lineage_item = {"column": output_column, "original_columns": list(real_columns)}

                if lineage_item not in self.column_lineage:
                    self.column_lineage.append(lineage_item)

    def _handle_column_lineage(self):
        """
        处理字段级血缘关系
        1. 提取 input_tables
        2. 重新生成 column_lineage
        """
        new_column_lineage = []
        input_table = "unknown"
        for i, item in enumerate(self.column_lineage):
            new_original_columns = []
            for col, _type in item["original_columns"]:
                table_name = ".".join(col.split(".")[:-1])
                if _type == "column":
                    input_table = table_name
                    new_original_columns.append(col)

                if _type in ["function", "literal"]:
                    new_original_columns.append(col)

                if _type == "star":
                    input_table = table_name
                    new_original_columns.append(col)

                # 添加输入表
                if input_table not in self.input_tables:
                    self.input_tables.append(input_table)

            try:
                new_output_column = self.target_columns[i]
            except IndexError:
                new_output_column = item["column"]
            new_item = {"column": new_output_column, "original_columns": new_original_columns}
            new_column_lineage.append(new_item)

        self.input_tables.remove("unknown") if "unknown" in self.input_tables else None
        return new_column_lineage


sql = """
select 
id,
name
from t1
union all
select 
id2,
name2
from t2
union all
select 
id3,
name3
from t3
union
select 
id4,
name4
from t4
"""


def test_extractor():
    extractor = ColumnLineageExtractor(sql, dialect="starrocks")
    lineage = extractor.extract()
    # print(extractor.target_table)
    # print(extractor.target_columns)
    # print(extractor.table_mapping)

    # print(extractor.column_mapping)
    # for i, x in enumerate(extractor.column_mapping):
    #     print(i, x)
    for i, x in enumerate(extractor.column_lineage):
        print(i, x)

    pprint(lineage)


def debug():
    for node in sqlglot.parse_one(sql, read="starrocks").walk(bfs=True):
        print(type(node), node.args)


if __name__ == "__main__":
    # test_extractor()
    debug()
