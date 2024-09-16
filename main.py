import et as et
import l as l
import pandas as pd
from datetime import datetime, timedelta
import pytz
import warnings
warnings.filterwarnings("ignore")

def runUK():
    report_id = et.get_report_id()
    # report_id = '261034019982'
    report_data = et.get_report_data(report_id)

    # Error patch
    report_data = report_data[report_data['sku'] != 'HA1107NB.']

    asins = list(report_data['asin'])
    sales_data_30 = et.get_sales_data(asins)
    sales_data_7 = et.get_sales_data(asins, days_before=7)

    skus = list(report_data['sku'])
    status_data = et.get_product_status(skus)
    

    df = pd.merge(report_data, sales_data_30, on='asin', how='left')
    df = pd.merge(df, sales_data_7, on='asin', how='left')
    df = pd.merge(df, status_data, on='sku', how='left')

    df = et.add_product_group(df)
    df = et.add_product_quant(df)

    dfs = et.clean(df)

    l.load_df('RawUK', dfs[0])
    l.load_df('ProductQuantUK', dfs[1])

def runDE():

    report_id = et.get_report_id(marketplace="DE")
    #report_id = '257896019974'
    report_data = et.get_report_data(report_id, marketplace="DE")

    asins = list(report_data['asin'])
    sales_data_30 = et.get_sales_data(asins, marketplace="DE")
    sales_data_7 = et.get_sales_data(asins, marketplace="DE", days_before=7)

    skus = list(report_data['sku'])
    status_data = et.get_product_status(skus, marketplace="DE")

    df = pd.merge(report_data, sales_data_30, on='asin', how='left')
    df = pd.merge(df, sales_data_7, on='asin', how='left')
    df = pd.merge(df, status_data, on='sku', how='left')

    df = et.add_product_group(df)
    df = et.add_product_quant(df)

    dfs = et.clean(df)

    l.load_df('RawDE', dfs[0])
    l.load_df('ProductQuantDE', dfs[1])    




if __name__ == '__main__':

    print('UK:\n')
    runUK()
    print('DE:\n')
    runDE()

    

