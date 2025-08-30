from collections import defaultdict
from contextlib import contextmanager

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

            self._resolve_column_lineage()
            self._finalize_lineage()

            return {
                "input_tables": list(set(self.input_tables)),
                "output_tables": self.target_table,
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
        if isinstance(node, exp.Insert):
            self._handle_insert_node(node)
        elif isinstance(node, exp.Select):
            self._handle_select_node(node)
        elif isinstance(node, exp.With):
            self._handle_with_node(node)
        elif isinstance(node, exp.Subquery):
            self._handle_subquery_node(node)
        elif isinstance(node, exp.Union):
            self._handle_union_node(node)

        # 递归处理子节点
        self._traverse_children(node)
        self.visited.add(node)

    def _handle_insert_node(self, node: exp.Insert):
        """处理 INSERT 节点"""
        if isinstance(node.this, exp.Table):
            self.target_table = node.this.name
        elif isinstance(node.this, exp.Schema):
            schema = node.this
            self.target_table = schema.this.name
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

        # 处理选择的列
        for select_expr in node.selects:
            alias = select_expr.alias or select_expr.this.sql()
            output = f"{self.current_scope}.{alias}"

            columns = list(select_expr.find_all(exp.Column))
            if columns:
                self._handle_column_expressions(columns, table_map, output)
            else:
                self._handle_non_column_expressions(select_expr, table_map, output)

    def _handle_column_expressions(self, columns, table_map, output):
        """处理列表达式"""
        for column in columns:
            real_table = table_map.get(column.table, column.table)
            full_column_name = f"{real_table}.{column.name}"

            if isinstance(column.this, exp.Star):
                output = f"{self.current_scope}.*"
                _type = "star"
            else:
                _type = "column"

            self.column_mapping.append({"input": full_column_name, "output": output, "type": _type})

    def _handle_non_column_expressions(self, select_expr, table_map, output):
        """处理非列表达式（如函数、字面量等）"""
        # 处理 select *
        if isinstance(select_expr, exp.Star):
            self._handle_star_expression(table_map, output)
        else:
            select_node = select_expr.this
            select_sql = select_node.sql()

            if isinstance(select_node, exp.Func):
                _type = "function"
            elif isinstance(select_node, exp.Literal):
                _type = "literal"
            else:
                _type = "expression"

            self.column_mapping.append({"input": select_sql, "output": output, "type": _type})

    def _handle_star_expression(self, table_map, output):
        """处理星号表达式"""
        _type = "star"
        output = f"{self.current_scope}.*"
        for table in table_map.values():
            select_sql = f"{table}.*"
            self.column_mapping.append({"input": select_sql, "output": output, "type": _type})

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

    def _handle_union_node(self, node: exp.Union):
        """处理 UNION 节点"""
        # UNION 不改变作用域，直接遍历子节点
        pass

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

    def _register_table(self, table, table_map: dict):
        """注册表到表映射中"""
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
        self.input_tables = []

        for i, item in enumerate(self.column_lineage):
            new_original_columns = self._process_original_columns(item["original_columns"])
            new_output_column = self._get_output_column(i, item["column"])

            new_item = {"column": new_output_column, "original_columns": new_original_columns}
            new_column_lineage.append(new_item)

        self.column_lineage = new_column_lineage

    def _process_original_columns(self, original_columns):
        """处理原始列信息，提取输入表"""
        processed_columns = []

        for col, _type in original_columns:
            if _type in ["column", "star"]:
                table_name = ".".join(col.split(".")[:-1])
                self._add_input_table(table_name)
            # function和literal类型不需要添加到输入表中

            processed_columns.append(col)

        return processed_columns

    def _add_input_table(self, table_name):
        """添加输入表到列表中（避免重复）"""
        if table_name and table_name != "unknown" and table_name not in self.input_tables:
            self.input_tables.append(table_name)

    def _get_output_column(self, index, default_column):
        """获取输出列名"""
        try:
            return self.target_columns[index]
        except IndexError:
            return default_column


# 测试代码
sql = """
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


def test_extractor():
    extractor = ColumnLineageExtractor(sql, dialect="mysql")
    lineage = extractor.extract()

    for i, x in enumerate(extractor.column_lineage):
        print(i, x)

    print(lineage)


if __name__ == "__main__":
    test_extractor()
