import et
import l
import pandas as pd
from datetime import datetime, timedelta
import pytz


if __name__ == '__main__':

    report_id = et.get_report_id()
    report_data = et.get_report_data(report_id)

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

    l.load_df('Raw', df)


