from src.column_lineage import ColumnLineageExtractor





sql = """
select id, name from t1
union all
select id2, name2 , dd from t2

"""


def test_extractor():
    extractor = ColumnLineageExtractor(sql, dialect="starrocks")
    lineage = extractor.extract()

    # print('target_table: ', extractor.target_table)
    # print("column_mapping: ", extractor.column_mapping)

    # for i, x in enumerate(extractor.column_lineage):
    #     print(i, x)

    # print(lineage)

    extractor.display_compact_rich_table(lineage)

    # extractor.display_compact_html_table(lineage)


if __name__ == "__main__":
    test_extractor()
