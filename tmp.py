import sqlglot
from sqlglot.optimizer.qualify import qualify

sql = """



"""


def debug():
    ast = sqlglot.parse_one(sql, read="starrocks")
    ast = qualify(ast)
    # print(ast._type)
    for node in ast.walk(bfs=True):
        print(type(node), node.args)


if __name__ == "__main__":
    # test_extractor()
    debug()
