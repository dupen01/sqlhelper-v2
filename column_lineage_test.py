from src.column_lineage import ColumnLineageExtractor





sql = """

-- 门店春语提取组成分析
INSERT OVERWRITE dws_hive.dws_shop_chunyu_extract_combination_analysis
select
    s.shop_code,
    s.shop_name,
    s.business_manager_code,
    s.business_manager_name,
    s.branch_manager_code,
    s.branch_manager_name,
    s.regional_manager_code,
    s.regional_manager_name,
    s.benchmarking_group,
    s.filiale_code,
    s.filiale_name,
    s.city_region_code,
    s.city_region_name,
    s.nationwide_region_code,
    s.nationwide_region_name,
    t1.statistics_date,
    '' item_id,
    t1.item_name,
    t1.incomes,
    t1.extract_nums,
    t1.extract_people_nums
from (
         SELECT
             p.xy_belong_shop_code shop_code,
             date_format(p.extract_time, '%Y-%m-%d')  statistics_date,
             pi.four_level item_name,
             sum(p.accounting_income) incomes,
             sum(p.extract_count) extract_nums,
             concat_ws (',', ARRAY_AGG(DISTINCT m.serial_num)) extract_people_nums
         FROM
             (
                 select 
               					-- IFNULL(xy_belong_shop_code, showyu_shop_code) xy_belong_shop_code,
               					IFNULL(xy_belong_shop_code, '') xy_belong_shop_code, -- 1225 老魏 
                        customer_erpid,
                        extract_time,
                        tax_base AS accounting_income,
                        extract_count,
                        asset_code
                 from ods_hive.ods_dw_cy_p7_new_merge
                 where is_staff = '否'
                   AND asset_extract_type IN ('资产提取', '退款收入')
                   AND (xy_belong_shop_code IS NOT NULL or showyu_shop_code is not null)
                 union all
                 select xy_belong_shop_code,
                        customer_erpid,
                        extract_time,
                        accounting_income,
                        extract_count,
                        asset_code
                 from dwd_hive.dwd_dw_cy_p7_new_staff_positive_income_temp
             )  p
                 inner join ods_hive.ods_member_merge m on m.id = p.customer_erpid
                 inner join (
                 select  assets_code,
                         if(four_level like '%点阵波%', '点阵波', four_level) four_level
                 from ods_hive.ods_cy_dim_product_info
             ) pi on p.asset_code = pi.assets_code

         group by p.xy_belong_shop_code, date_format(p.extract_time, '%Y-%m-%d'), pi.four_level
     ) t1
         inner join dwd_hive.dwd_shop_info_detail s on s.shop_code = t1.shop_code
where s.shop_code not like '%JM%'
;

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
