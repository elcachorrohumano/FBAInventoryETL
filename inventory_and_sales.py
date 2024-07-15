import json
import time
import csv
from datetime import datetime, timedelta
import pytz

import pandas as pd
import requests
from sp_api.api import Reports, Sales
from sp_api.base import Marketplaces, ReportType, ProcessingStatus, Granularity

from typing import List
import requests
import pandas as pd

from credentials import credentials
from product_groups import product_groups

# Function to determine product group based on the product name
def get_product_group(product_name):
    for group, products in product_groups.items():
        if product_name in products:
            return group
    return "Unknown"




if __name__ == '__main__':

    report_type = ReportType.GET_FBA_MYI_ALL_INVENTORY_DATA
    res = Reports(credentials=credentials, marketplace=Marketplaces.UK)
    data = res.create_report(reportType=report_type)
    report = data.payload

    report_id = report['reportId']
    print('Got the report id')

    res = Reports(credentials=credentials, marketplace=Marketplaces.UK)
    data = res.get_report(report_id)
    while data.payload.get('processingStatus') not in [ProcessingStatus.DONE, ProcessingStatus.FATAL, ProcessingStatus.CANCELLED]:
        print('Waiting for report to be processed...')
        time.sleep(2)
        data = res.get_report(report_id)
    
    if data.payload.get('processingStatus') in [ProcessingStatus.FATAL, ProcessingStatus.CANCELLED]:
        print('Report processing failed: ', data.payload.get('processingStatus'))
        report_data = data.payload

    else:
        print('Successfully downloaded report data')
        report_data = res.get_report_document(data.payload['reportDocumentId'])
    
    report_url = report_data.payload['url']

    res = requests.get(report_url)

    decoded_content = res.content.decode('cp1252')
    reader = csv.DictReader(decoded_content.splitlines(), delimiter='\t')

    data_list = list(reader)
    inventory = pd.DataFrame(data_list)
    
    timezone = pytz.timezone('Europe/London')
    end_date = datetime.now(timezone)
    start_date = end_date - timedelta(days=30)
    start_date_str = start_date.isoformat()
    end_date_str = end_date.isoformat()

    asins = list(inventory['asin'])
    marketplaces = dict(UK=Marketplaces.UK)
    data = []
    for asin in asins:
        for country, marketplace_id in marketplaces.items():
            sales = Sales(credentials=credentials, marketplace=marketplace_id)
            res = sales.get_order_metrics(
                interval=(start_date_str, end_date_str),
                granularity=Granularity.TOTAL,
                asin=asin)
            metrics = res.payload[0]
            data.append({'asin': asin,
                         'unit_count': metrics['unitCount'],
                         #'order_item_count': metrics['orderItemCount'],
                         #'order_count': metrics['orderCount'],
                         #'country': country,
                         })
            print(f'Got the sales for asin {asin}')
            time.sleep(2)
    print('Succesfully got the sales data')
    sales = pd.DataFrame(data)

    df = pd.merge(inventory, sales, on='asin', how='left')


    df.to_csv('reports/data.csv', index=False)
    print('Finished writing the data to reports/data.csv')

    # Add 'Product Group' column based on the product name
    df['Product Group'] = df['product-name'].apply(get_product_group)

    # Step 2: Select and rename the relevant columns
    df_selected = df[['Product Group', 'product-name', 'sku', 'afn-fulfillable-quantity',
                    'afn-inbound-shipped-quantity', 'afn-inbound-receiving-quantity', 
                    'afn-inbound-working-quantity', 'unit_count']].copy()

    df_selected.rename(columns={
        'product-name': 'Product',
        'sku': 'SKU',
        'afn-fulfillable-quantity': 'Available',
        'afn-inbound-shipped-quantity': 'Shipped',
        'unit_count': 'Units Sold T-30 Days'
    }, inplace=True)

    # Step 3: Convert necessary columns to numeric, coerce errors to NaN (if any)
    df_selected['Units Sold T-30 Days'] = pd.to_numeric(df_selected['Units Sold T-30 Days'], errors='coerce')
    df_selected['Available'] = pd.to_numeric(df_selected['Available'], errors='coerce')
    df_selected['Shipped'] = pd.to_numeric(df_selected['Shipped'], errors='coerce')
    df['afn-inbound-receiving-quantity'] = pd.to_numeric(df['afn-inbound-receiving-quantity'], errors='coerce')
    df['afn-inbound-working-quantity'] = pd.to_numeric(df['afn-inbound-working-quantity'], errors='coerce')

    # Step 4: Compute the 'Processing' column
    df_selected['Processing'] = df['afn-inbound-receiving-quantity'] + df['afn-inbound-working-quantity']

    # Step 5: Compute the 'Total Inventory' column
    df_selected['Total Inventory'] = df_selected['Available'] + df_selected['Shipped'] + df_selected['Processing']

    df_selected.drop(columns=['afn-inbound-receiving-quantity', 'afn-inbound-working-quantity'], inplace=True)
    df_selected = df_selected[['Product Group',
                            'Product',
                            'SKU',
                            'Available',
                            'Shipped',
                            'Processing',
                            'Total Inventory',
                            'Units Sold T-30 Days']]
    
    df_selected.to_csv('reports/clean_data.csv', index=False)

    print('Finished writing the data to reports/clean_data.csv')

