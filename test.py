import pandas as pd
import numpy as np
import os
from datetime import datetime

cartype_local={
    '唐EV':'长抚',
    '全新一代唐EV':'长抚',
    '海豚':'长抚',
    '腾势D9 EV':'长抚',
    'e2/e3':'长抚',
    'e6':'长抚',
    '唐DM-i':'长抚',
    '宋MAX DM-i':'长抚',
    '腾势D9 DM-i':'长抚',
    '驱逐舰05':'长抚',
    '腾势N8':'长抚',
    '元PLUS':'合肥',
    '秦PLUS EV':'合肥',
    '秦PLUS DM-i':'合肥',
    '元Pro':'合肥',
    '元UP':'合肥',
    '秦L DM-i':'合肥',
    '海豹06DM-i':'合肥',
    '海豹':'常州',
    '海狮07EV':'常州',
    '宋PLUS EV':'西商',
    '全新秦EV':'西商',
    '宋PLUS DM-i':'西商',
    '护卫舰07':'西商',
    '海鸥':'西商',
    'U8':'西商',
    '海豹DM-i':'郑州',
    '宋Pro DM-i':'郑州',
    '豹5':'郑州',
    '腾势N7':'济南',
    '宋L':'济南',
    '汉EV':'深汕',
    '汉DM-i':'深汕',
    '宋L DM-i':'郑州',
}

def read_excel(file):
    df = pd.read_excel(file, sheet_name=0)
    file_name = os.path.basename(file)[:-5]
    df_input_cut = df[['工厂','工厂_地区','QMS车型','服务店城市','零部件大类','备件名称','VIN','故障描述','故障原因','是否救援','维修措施','行驶里程','电里程','生产日期','索赔申请日期','工单开单日期','责任PQE']].copy()
    # df_input_cut['大数据确认(行驶/静止/无法识别)'] = np.nan
    # df_input_cut['PQE是否同意复议(能/否)'] = np.nan
    df_input_cut['备注'] = np.nan
    df_input_cut.insert(3,'地区_品保',np.nan)
    df_input_cut.insert(0,'latest','y')
    return df_input_cut, file_name

def deal_df(df_input_cut, input_file, out_file):
    file_path = input_file.replace("/pminput/","/pmdynamicout/")
    dirname = os.path.dirname(file_path)
    basename = file_path.rsplit('.', 1)[0]
    basename = f'{basename}.xlsx'
    
    if os.path.exists(out_file):
        df_out_file = pd.read_excel(out_file, dtype={'VIN': str}, sheet_name=0)
        df_out_file['VIN'] = df_out_file['VIN'].astype('str')
        df_input_cut['索赔申请日期'] = df_input_cut['索赔申请日期'].astype(str)
        df_out_file['索赔申请日期'] = df_out_file['索赔申请日期'].astype(str)
        df_out_file['latest'] = np.nan

        # for value in df_input_cut['索赔申请日期']:
        #     print(type(value))
        # for value in df_out_file['索赔申请日期']:
        #     print(type(value))
        # print(df_input_cut.info())
        # print("---")
        # print(df_out_file.info())
        # 定义用来判断数据是否重复的字段
        # key_columns = ['地区_品保','工厂', '工厂_地区', 'QMS车型', '服务店城市', '零部件大类', '备件名称', 'VIN', '故障描述', '故障原因', 
        #                '行驶里程', '电里程', '索赔申请日期', '工单开单日期', '责任PQE']
        # key_columns = ['工厂', '工厂_地区', 'QMS车型', '服务店城市', '零部件大类', '备件名称', 'VIN', '故障描述', '故障原因', '行驶里程', '电里程', '生产日期', '索赔申请日期', '工单开单日期']
        key_columns = ['工厂', '工厂_地区', 'QMS车型', '服务店城市', '零部件大类', '备件名称', 'VIN', '故障描述', '故障原因', '行驶里程', '电里程', '索赔申请日期', '工单开单日期']

        # 找到新数据和旧数据的并集，保留旧数据的其他字段
        merged_df = pd.merge(df_out_file, df_input_cut, on=key_columns, how='outer', suffixes=('_旧', '_新'))

        # 用旧数据的非关键字段填补新数据中的NaN值
        for col in df_out_file.columns:
            if col not in key_columns:
                merged_df[col] = merged_df[f'{col}_旧'].combine_first(merged_df[f'{col}_新'])

        # 删除多余的列
        merged_df = merged_df[df_out_file.columns]

    else:
        merged_df = df_input_cut

    # 新增加的，加上一个时间
    current_date = datetime.now().strftime('%Y-%m-%d')
    for index,row in merged_df.iterrows():
        # print(row["地区_品保"],row['备注'])
        if pd.isna(row["地区_品保"]):
            row['备注'] = f'{current_date}新增。'
        merged_df.iloc[index] = row

    # 应用字典进行填充"地区_品保"字段
    merged_df['地区_品保'] = merged_df['QMS车型'].map(cartype_local)

    # 如果输出目录不存在，创建它
    if not os.path.exists(dirname):
        os.makedirs(dirname)

    # for value in merged_df['索赔申请日期']:
    #     print(type(value))

    merged_df = merged_df.sort_values(by='索赔申请日期')
    merged_df.to_excel(basename, index=False)

def main():
    input_file = './pminput/202411/1-24.xlsx'  # 最新的行驶抛锚
    out_file = './pmdynamicout/202411/1-23.xlsx'  # 以前维护的数据，脚本运行之前记得手动更新此数据
    df_input_cut, file_name = read_excel(input_file)
    deal_df(df_input_cut, input_file, out_file)

if __name__ == "__main__":
    main()