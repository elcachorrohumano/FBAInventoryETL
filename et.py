import json
import time
import csv
from datetime import datetime, timedelta
import pytz

import pandas as pd
import requests
from sp_api.api import Reports, Sales, ListingsItems
from sp_api.base import Marketplaces, ReportType, ProcessingStatus, Granularity

from typing import List
import requests
import pandas as pd

from keys.amazon_credentials import credentials
from product_info.product_groups import product_groups
from product_info.product_quantification import product_quant


def get_report_id(type="GET_FBA_MYI_ALL_INVENTORY_DATA", marketplace="UK") -> str:
    """
    Function to get the report id for a specific report type and marketplace
    """
    if type=="GET_FBA_MYI_ALL_INVENTORY_DATA":
        report_type = ReportType.GET_FBA_MYI_ALL_INVENTORY_DATA
        if marketplace=="UK":
            marketplace = Marketplaces.UK
        elif marketplace=="DE":
            marketplace = Marketplaces.DE
    res = Reports(credentials=credentials, marketplace=marketplace)
    data = res.create_report(reportType=type)
    report = data.payload
    report_id = report['reportId']
    print(f'Successfully got the report id: {report_id}')
    return report_id


def get_report_data(report_id: str, marketplace="UK") -> pd.DataFrame:
    """
    Function to get the report dataframe for a specific report id
    """
    if marketplace=="UK":
        marketplace_id = Marketplaces.UK
    elif marketplace=="DE":
        marketplace_id = Marketplaces.DE
    res = Reports(credentials=credentials, marketplace=marketplace_id)
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
    df = pd.DataFrame(data_list)
    df['marketplace'] = marketplace

    return df

def get_sales_data(asins: list,  marketplace="UK", end_date=datetime.now(pytz.timezone('Europe/London')), days_before=30) -> pd.DataFrame:
    """
    Function to get the sales from a list of asins data for a specific date range
    """
    if marketplace=="UK":
        marketplace = Marketplaces.UK
    elif marketplace=="DE":
        marketplace = Marketplaces.DE


    start_date = end_date - timedelta(days=days_before)
    start_date_str = start_date.isoformat()
    end_date_str = end_date.isoformat()
    
    data = []
    for asin in asins:
        sales = Sales(credentials=credentials, marketplace=marketplace)
        res = sales.get_order_metrics(
            interval=(start_date_str, end_date_str),
            granularity=Granularity.TOTAL,
            asin=asin
            )
        metrics = res.payload[0]
        data.append({'asin': asin,
                     f'unit_count_{days_before}_days': metrics['unitCount'],
                     f'order_item_count_{days_before}_days': metrics['orderItemCount'],
                     f'order_count_{days_before}_days': metrics['orderCount'],
                     f'averageUnitPrice_{days_before}_days': metrics['averageUnitPrice'],
                     f'totalSales_{days_before}_days': metrics['totalSales'],
                     })
        time.sleep(2)
    sales = pd.DataFrame(data)
    sales[f'averageUnitPrice_{days_before}_days'] = sales[f'averageUnitPrice_{days_before}_days'].apply(lambda x: x['amount'])
    sales[f'currencyCode_{days_before}_days'] = sales[f'totalSales_{days_before}_days'].apply(lambda x: x['currencyCode'])
    sales[f'totalSales_{days_before}_days'] = sales[f'totalSales_{days_before}_days'].apply(lambda x: x['amount'])
    print(f'Succesfully got the sales data from the past {days_before} days')
    return sales

def get_product_status(skus: list, marketplace="UK") -> pd.DataFrame:
    """
    Function to get the product status from a list of skus
    """
    if marketplace == "UK":
        marketplace = Marketplaces.UK
    elif marketplace == "DE":
        marketplace = Marketplaces.DE
    
    statuses = []
    for sku in skus:
        items = ListingsItems(credentials=credentials, marketplace=marketplace)
        res = items.get_listings_item(sellerId='A1VGZFRQE55Z7K', sku=sku)
        statuses.append({'sku': sku,
                         'status_amazon': res.payload['summaries'][0]['status']})
        time.sleep(0.5)
    
    statuses = pd.DataFrame(statuses)
    statuses['status_amazon'] = statuses['status_amazon'].apply(lambda x: ', '.join(map(str, x)))
    
    statuses['Status'] = statuses['status_amazon'].apply(
        lambda x: "Active" if x in ["BUYABLE, DISCOVERABLE", "DISCOVERABLE, BUYABLE"] 
                  else "Inactive" if x == "DISCOVERABLE" 
                  else "Suppressed" if x == "" 
                  else "Unknown"
    )
    
    print('Successfully got the statuses for all skus')
    return statuses


def get_product_group(product_name: str) -> str:
    """
    Function to get the product group based on the product name
    """
    for group, products in product_groups.items():
        if product_name in products:
            return group
    return "Unknown"

def add_product_group(df: pd.DataFrame) -> pd.DataFrame:
    """
    Function to add the product group column to the dataframe
    """
    df['Product Group'] = df['product-name'].apply(get_product_group)
    print('Successfully added the product group column')
    return df

def add_product_quant(df: pd.DataFrame) -> pd.DataFrame:
    """
    Function to add the product count of each bundle
    """
    product_quant_df = pd.DataFrame.from_dict(product_quant, orient='index').reset_index().rename(columns={'index': 'sku'})
    df = pd.merge(df, product_quant_df, on='sku' ,how='left')
    print('Successfully added the product quantification')
    return df