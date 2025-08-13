from .keywords import KeyWords


class ParseException(Exception):
    pass


class SqlHelper:
    @staticmethod
    def split(sql: str) -> list[str]:
        """将多条SQL以 `;` 作为分隔符进行划分，返回列表"""
        result = []
        # 嵌套注释的层级数
        depth = 0
        # 多行SQL的前缀语句,分号之前的语句
        prefix = ""
        sql = sql + ";" if not sql.strip().endswith(";") else sql
        for line in sql.splitlines():
            line = line if not line.strip().startswith("--") else ""
            # 标记是否以双引号结尾
            has_terminated_double_quote = True
            # 标记是否以单引号结尾
            has_terminated_single_quote = True
            # 标记是否属于单行注释内容
            is_single_line_comment = False
            # 标记前一个字符是否是短横行 "-"
            was_pre_dash = False
            # 标记前一个字符是否是斜杆 "/"
            was_pre_slash = False
            # 标记前一个字符是否是星号 "*"
            was_pre_star = False
            last_semi_index = 0
            index = 0
            if len(prefix) > 0:
                prefix += "\n"
            for char in line:
                index += 1
                match char:
                    case "'":
                        if has_terminated_double_quote:
                            has_terminated_single_quote = not has_terminated_single_quote
                    case '"':
                        if has_terminated_single_quote:
                            has_terminated_double_quote = not has_terminated_double_quote
                    case "-":
                        if has_terminated_double_quote and has_terminated_single_quote:
                            if was_pre_dash:
                                is_single_line_comment = True
                        was_pre_dash = True
                    case "/":
                        if has_terminated_double_quote and has_terminated_single_quote:
                            # 如果'/'前面是'*'， 那么嵌套层级数-1
                            if was_pre_star:
                                depth -= 1
                        was_pre_slash = True
                        was_pre_dash = False
                        was_pre_star = False
                    case "*":
                        if has_terminated_double_quote and has_terminated_single_quote:
                            # 如果'*'前面是'/'， 那么嵌套层级数+1
                            if was_pre_slash:
                                depth += 1
                        was_pre_star = True
                        was_pre_dash = False
                        was_pre_slash = False
                    case ";":
                        # 当分号不在单引号内，不在双引号内，不属于单行注释，并且多行嵌套注释的层级数为0时，表示此分号应该作为分隔符进行划分
                        if (
                            has_terminated_double_quote
                            and has_terminated_single_quote
                            and not is_single_line_comment
                            and depth == 0
                        ):
                            sql_stmt = prefix + line[last_semi_index : index - 1]
                            result.append(sql_stmt)
                            prefix = ""
                            last_semi_index = index
                    case _:
                        was_pre_dash = False
                        was_pre_slash = False
                        was_pre_star = False
            if last_semi_index != index or len(line) == 0:
                prefix += line[last_semi_index:]
        assert depth == 0, f"The number of nested levels of sql multi-line comments is not equal to 0: {depth}"
        if "" in result:
            result.remove("")
        return result

    def trim_comment(self, sql: str) -> str:
        """删除注释"""
        # 1. 删除单行注释
        sql = self.__trim_single_line_comment(sql=sql)

        # 2. 将多行SQL转为单行SQL
        sql = "\\n".join(sql.splitlines())

        # 3. 删除多行注释
        index = 0
        # 嵌套注释的层级数
        depth = 0
        # 标记是否以双引号结尾
        has_terminated_double_quote = True
        # 标记是否以单引号结尾
        has_terminated_single_quote = True
        # 标记前一个字符是否是斜杆 "/"
        was_pre_slash = False
        # 标记前一个字符是否是星号 "*"
        was_pre_star = False
        # 标记是否是SQL Hint
        is_hint = False
        comment_start_index = 0
        comment_end_index = 0
        comment_index_list = []
        for char in sql:
            index += 1
            match char:
                case "'":
                    if has_terminated_double_quote:
                        has_terminated_single_quote = not has_terminated_single_quote
                case '"':
                    if has_terminated_single_quote:
                        has_terminated_double_quote = not has_terminated_double_quote
                case "/":
                    if has_terminated_double_quote and has_terminated_single_quote:
                        # 如果'/'前面是'*'， 那么嵌套层级数-1
                        if was_pre_star:
                            if not is_hint:
                                depth -= 1
                                if depth == 0:
                                    comment_end_index = index
                                    comment_index_list.append((comment_start_index, comment_end_index))
                            else:
                                is_hint = False
                    was_pre_slash = True
                    was_pre_star = False
                case "*":
                    if has_terminated_double_quote and has_terminated_single_quote:
                        # 如果'*'前面是'/'， 那么嵌套层级数+1
                        if was_pre_slash:
                            depth += 1
                            # 记录层级为1的开始索引
                            if depth == 1:
                                comment_start_index = index - 2
                    was_pre_star = True
                    was_pre_slash = False
                case "+":
                    if has_terminated_double_quote and has_terminated_single_quote:
                        if was_pre_star and depth == 1:
                            is_hint = True
                            depth = 0
                    was_pre_star = False
                    was_pre_slash = False
                case _:
                    was_pre_slash = False
                    was_pre_star = False
        for start, end in reversed(comment_index_list):
            sql = sql[:start] + sql[end:]
        # 4. 单行SQL转为多行
        sql = sql.replace("\\n", "\n")
        return sql

    def __trim_single_line_comment(self, sql: str) -> str:
        """删除单行注释"""
        result = []
        for line in sql.splitlines():
            line = line.strip()
            line = line if not line.startswith("--") else ""
            line = line if not line.startswith("#") else ""
            if len(line) == 0:
                continue
            # 标记是否以双引号结尾
            has_terminated_double_quote = True
            # 标记是否以单引号结尾
            has_terminated_single_quote = True
            # 标记前一个字符是否是短横行 "-"
            was_pre_dash = False
            index = 0
            for char in line:
                index += 1
                match char:
                    case "'":
                        if has_terminated_double_quote:
                            has_terminated_single_quote = not has_terminated_single_quote
                    case '"':
                        if has_terminated_single_quote:
                            has_terminated_double_quote = not has_terminated_double_quote
                    case "-":
                        if has_terminated_double_quote and has_terminated_single_quote:
                            if was_pre_dash:
                                line = line[: index - 2]
                                continue
                        was_pre_dash = True
                    case "#":
                        if has_terminated_double_quote and has_terminated_single_quote:
                            line = line[: index - 1]
                            continue
                    case _:
                        was_pre_dash = False
            result.append(line)
        return "\n".join(result)

    def __get_cte_mid_tables(self, sql: str) -> list:
        """获取cte语句的临时表名"""
        # 括号层级
        bracket_level = 0
        was_pre_with = False
        is_cte = False
        was_pre_right_bracket = False
        result = []

        # 预处理：去掉多行注释和单行注释
        sql = self.trim_comment(sql)

        for line in sql.splitlines():
            line = line.strip()
            if len(line) == 0:
                continue

            line = line.replace("(", " ( ")
            line = line.replace(")", " ) ")
            line = line.replace(",", " , ")

            for token in line.split(" "):
                token = token.strip()
                if len(token) == 0:
                    continue
                if token.upper() == "(":
                    bracket_level += 1
                if token.upper() == ")":
                    bracket_level -= 1
                    was_pre_right_bracket = True
                if token.upper() == "WITH":
                    was_pre_with = True
                    is_cte = True
                    continue

                if token.upper() in KeyWords.keywords:
                    if was_pre_right_bracket and is_cte and bracket_level == 0 and token.upper() != "AS":
                        is_cte = False

                if token.upper() not in KeyWords.keywords:
                    if was_pre_with:
                        result.append(token)
                    if is_cte and bracket_level == 0 and not was_pre_with and token not in (",", "(", ")"):
                        result.append(token)
                    was_pre_with = False
        return result

    def get_source_target_tables(self, sql: str) -> dict | None:
        """传入一个SQL语句，输出这条SQL的来源表和目标表名，可用于表级血缘关系梳理
        TODO 暂未支持嵌套CTE语句
        """

        # 预处理：去掉多行注释和单行注释
        sql = self.trim_comment(sql).strip()
        # 删除末尾的`;`
        sql = sql[:-1] if sql.endswith(";") else sql

        # 校验SQL参数
        if len(SqlHelper.split(sql)) > 1:
            raise ParseException("sql脚本为多条SQL语句,需传入单条SQL语句.")

        was_pre_insert = False
        was_pre_from = False
        was_pre_as = False
        was_merge = False
        was_using = False
        was_pre_table_name = False
        was_pre_table_function = False
        target_table = []
        source_table = []
        result = {}

        for line in sql.splitlines():
            line = line.strip()
            if len(line) == 0:
                continue

            line = line.replace("(", " ( ")
            line = line.replace(")", " ) ")
            line = line.replace(",", " , ")

            for token in line.split(" "):
                token = token.strip()
                if len(token) == 0:
                    continue

                if token.upper() == "AS":
                    was_pre_as = True
                    continue

                if token.upper() in KeyWords.insert_keywords:
                    was_pre_insert = True
                    was_pre_from = False
                    continue

                if token.upper() == "MERGE":
                    was_merge = True
                    continue

                if token.upper() == "USING":
                    was_using = True
                    continue

                if token.upper() in KeyWords.from_keywords:
                    was_pre_from = True
                    was_pre_insert = False
                    was_pre_table_name = False
                    continue

                if was_pre_as and token.upper() not in KeyWords.keywords:
                    was_pre_as = False
                    was_pre_table_name = False
                    continue

                if token.upper() in KeyWords.keywords:
                    if was_pre_insert or was_pre_from:
                        was_pre_from = False
                    continue

                if token.upper() not in KeyWords.keywords and was_pre_insert:
                    target_table.append(token)
                    was_pre_insert = False
                    was_pre_from = False
                    continue

                if token.upper() in KeyWords.table_function_keywords and was_pre_from:
                    was_pre_table_function = True
                    continue

                # merge into
                if was_merge and not was_using and token.upper() not in KeyWords.keywords and len(target_table) == 0:
                    target_table.append(token)
                    continue

                if was_merge and was_using and token.upper() not in KeyWords.keywords:
                    if token != "(":
                        source_table.append(token)
                    was_using = False
                    was_merge = False
                    continue

                if was_pre_from:
                    if (
                        token not in KeyWords.keywords
                        and not was_pre_table_name
                        and token not in (",", "(")
                        and not was_pre_table_function
                    ):
                        source_table.append(token)
                        was_pre_from = True
                        was_pre_table_name = True
                    if token in ["AS", ","]:
                        was_pre_from = True
                        was_pre_table_name = False

        mid_table = self.__get_cte_mid_tables(sql)
        source_table = list(set(source_table) - set(mid_table))
        if len(source_table) != 0:
            result.setdefault("target_table", target_table)
            result.setdefault("source_table", source_table)
            return result
        else:
            return
