import pandas as pd
import sqlite3
import logging
from ingestion_db import ingest_db

logging.basicConfig(
    filename="logs/get_vendor_summary.log",
    level=logging.DEBUG,
    format="%(ascitime)s - %(levelname)s - %(message)s",
    filemode="a"
)

def create_vendor_summary(connection):
    total_vendor_sales_summary = pd.read_sql_query("""WITH FreightSummary AS (

    SELECT
        VendorNumber,
        SUM(Freight) AS FreightCost
    FROM vendor_invoice
    GROUP BY VendorNumber
),

purchaseSummary AS (
    SELECT
        p.VendorNumber,
        p.VendorName,
        p.Brand,
        p.PurchasePrice,
        p.Description,
        pp.Volume,
        pp.Price AS ActualPrice,
        SUM(p.Quantity) AS totalPurchaseQuantity,
        SUM(p.Dollars) AS totalPurchaseDollars
    FROM purchases p
    JOIN purchase_prices pp
        ON p.Brand = pp.Brand
    WHERE p.PurchasePrice > 0
    Group By p.VendorName, p.VendorNumber, p.Brand, p.Description, p.PurchasePrice, pp.Price, pp.Volume
),

SalesSummary AS (
    SELECT
        VendorNo,
        Brand,
        SUM(SalesDollars) as TotalSalesDollars,
        SUM(SalesPrice) as TotalSalesPrice,
        SUM(SalesQuantity) as TotalSalesQuantity,
        SUM(ExciseTax) as TotalExciseTax
    FROM sales
    GROUP BY VendorNo,Brand
)

    SELECT
        ps.VendorNumber,
        ps.VendorName,
        ps.Brand,
        ps.Description,
        ps.PurchasePrice,
        ps.ActualPrice,
        ps.Volume,
        ps.totalPurchaseQuantity,
        ps.totalPurchaseDollars,
        ss.TotalSalesQuantity,
        ss.TotalSalesDollars,
        ss.TotalSalesPrice,
        ss.TotalExciseTax,
        fs.FreightCost
    FROM PurchaseSummary ps
    LEFT JOIN SalesSummary ss
        ON ps.VendorNumber  = ss.VendorNo
        AND ps.Brand = ss.Brand
    LEFT JOIN FreightSummary fs
        ON ps.VendorNumber = fs.VendorNumber
    ORDER BY ps.TotalPurchaseDollars DESC""",connection)

    return total_vendor_sales_summary

def clean_data(df):
    '''cleaning the data'''
    df['Volume'] = df['Volume'].astype('float')
    df.fillna(0,inplace=True)

    df['VendorName'] = df['VendorName'].str.strip()
    df['Description'] = df['Description'].str.strip()

    #creating new columns for better analysis
    total_vendor_sales_summary['GrossProfit'] = total_vendor_sales_summary['TotalSalesDollars'] - total_vendor_sales_summary['totalPurchaseDollars']
    total_vendor_sales_summary['ProfitMargin'] = (total_vendor_sales_summary['GrossProfit'] / total_vendor_sales_summary['TotalSalesDollars'])*100
    total_vendor_sales_summary['StockTurnover'] = total_vendor_sales_summary['TotalSalesQuantity']/total_vendor_sales_summary['totalPurchaseQuantity']
    total_vendor_sales_summary['SalestoPurchaseRatio'] = total_vendor_sales_summary['TotalSalesDollars']/total_vendor_sales_summary['totalPurchaseQuantity']

    return df

if __name__ == 'main':
    connection = sqlite3.connect('inventory.db')

    logging.info('Creating Vendor Summary Table.....')
    summary_df=create_vendor_summary(connection)
    logging.info(summary_df.head())

    logging.info('Cleaning Data.....')
    clean_df=clean_data(summary_df)
    logging.info(clean_df.head())

    logging.info('Ingestion data.....')
    ingest_db.clean_df('total_vendor_sales_summary',connection)
    logging.info('Completed')