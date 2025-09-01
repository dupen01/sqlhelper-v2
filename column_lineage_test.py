from src.column_lineage import ColumnLineageExtractor



# 测试代码
"""
insert into t0
select t1.*, t11.name  
from t1
join t11 on t1.id = t11.id
union all
select id2, name2 from t2
union all
select  * from t3
;

insert into t0
select * from t1
union all
select id2, name2 from t2
union all
select  * from t3
"""


sql = """

insert into t0
 (aaa, bbb)
select id, name from t1
union all
select id2, name2 from t2
union all
select  id3, name3 from t3


"""


def test_extractor():
    extractor = ColumnLineageExtractor(sql, dialect="starrocks")
    lineage = extractor.extract()

    print('target_columns: ', extractor.target_columns)
    print("column_mapping: ", extractor.column_mapping)

    for i, x in enumerate(extractor.column_lineage):
        print(i, x)

    print(lineage)

    extractor.display_compact_rich_table(lineage)

    # extractor.display_compact_html_table(lineage)


if __name__ == "__main__":
    test_extractor()
