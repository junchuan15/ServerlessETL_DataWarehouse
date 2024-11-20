import base64
import json
import pandas as pd
from google.cloud import bigquery
import featuretools as ft

# BigQuery dataset
DATAWAREHOUSE = "data-warehouse-441704.Ecommerce_DW"

# Initialize BigQuery client globally
client = bigquery.Client()

def ETLpipeline(event, context):
    """
    Function to decode Pub/Sub message, perform feature engineering, and load data into BigQuery.
    """
    try:
        # Step 1: Extract data by Decoding the Pub/Sub message into JSON data
        pubsub_message = base64.b64decode(event['data']).decode('utf-8')
        print(f"Decoded Pub/Sub message: {pubsub_message}")
        data = json.loads(pubsub_message)

        # Convert JSON to pandas DataFrame
        df = pd.DataFrame([data])

        # 1. Customers Table
        customers_df = df[['Customer ID', 'Customer Name', 'Segment', 'Country', 'City', 'State', 'Postal Code', 'Region']].drop_duplicates(subset=['Customer ID']).reset_index(drop=True)

        # 2. Products Table
        products_df = df[['Product ID', 'Category', 'Sub-Category', 'Product Name']].drop_duplicates(subset=['Product ID']).reset_index(drop=True)

        # 3. Orders Table
        orders_df = df[['Order ID', 'Order Date', 'Ship Date', 'Ship Mode']].drop_duplicates().reset_index(drop=True)

        # 4. OrderDetails Table
        orderDetails_df = df[['Order ID', 'Product ID', 'Customer ID', 'Sales', 'Quantity', 'Discount', 'Profit']].drop_duplicates(subset=['Order ID', 'Product ID']).reset_index(drop=True)
        orderDetails_df['OrderDetail ID'] = orderDetails_df['Order ID'].astype(str) + "_" + orderDetails_df['Product ID'].astype(str)


        # Step 2: Perform feature engineering using Featuretools
        # CREATE EMPTY ENTITY SET
        es = ft.EntitySet(id="Superstore")

        # ADD DATA INTO ENTITY SET
        customers_entity = es.add_dataframe(dataframe=customers_df, dataframe_name='Customers',  index='Customer ID')
        products_entity = es.add_dataframe(dataframe=products_df,  dataframe_name='Products',  index='Product ID'  )
        orders_entity = es.add_dataframe(dataframe=orders_df,  dataframe_name='Orders',  index='Order ID')
        orderDetails_entity = es.add_dataframe(dataframe=orderDetails_df,  dataframe_name='OrderDetails',  index='OrderDetail ID')

        # DEFINE RELATIONSHIP
        relationships = [
            ('Customers', 'Customer ID', 'OrderDetails', 'Customer ID'),
            ('Products', 'Product ID', 'OrderDetails', 'Product ID'),
            ('Orders', 'Order ID', 'OrderDetails', 'Order ID')
        ]

        es.add_relationships(relationships)

        # APPLY DFS
        feature_matrix, feature_defs = ft.dfs(
            entityset=es,
            target_dataframe_name="OrderDetails",
            verbose=True,
            max_depth=2
        )

        # Select useful features
        selected_features_df = feature_matrix[[
            "Customers.COUNT(OrderDetails)",
            "Products.COUNT(OrderDetails)", "Products.MAX(OrderDetails.Profit)", "Products.MEAN(OrderDetails.Discount)",
            "Products.SUM(OrderDetails.Quantity)", "Products.SUM(OrderDetails.Sales)",
            "Orders.COUNT(OrderDetails)", "Orders.MAX(OrderDetails.Quantity)",
            "Orders.MEAN(OrderDetails.Profit)", "Orders.SUM(OrderDetails.Sales)",
            "Orders.MONTH(Order Date)", "Orders.YEAR(Order Date)"
        ]]

        selected_features_df = selected_features_df.rename(columns={
            "Customers.COUNT(OrderDetails)": "Customer_Order_Count",
            "Products.COUNT(OrderDetails)": "Product_Order_Count",
            "Products.MAX(OrderDetails.Profit)": "Product_Max_Profit",
            "Products.MEAN(OrderDetails.Discount)": "Product_Mean_Discount",
            "Products.SUM(OrderDetails.Quantity)": "Product_Total_Quantity",
            "Products.SUM(OrderDetails.Sales)": "Product_Total_Sales",
            "Orders.COUNT(OrderDetails)": "Order_Item_Count",
            "Orders.MAX(OrderDetails.Quantity)": "Order_Max_Quantity",
            "Orders.MEAN(OrderDetails.Profit)": "Order_Mean_Profit",
            "Orders.SUM(OrderDetails.Sales)": "Order_Total_Sales",
            "Orders.MONTH(Order Date)": "Order_Month",
            "Orders.YEAR(Order Date)": "Order_Year"
        })


        orderDetails_df = orderDetails_df.merge(
            selected_features_df,
            on="OrderDetail ID",
            how="left"
        )
        
        orderDetails_df = orderDetails_df.drop(columns=["OrderDetail ID"])

         # Step 3: Load data into BigQuery as DataWarehouse
        def load_to_bigquery(df, table_name):
            table_id = f"{DATAWAREHOUSE}.{table_name}"
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
            client.load_table_from_dataframe(df, table_id, job_config=job_config).result()
            print(f"Data successfully loaded into BigQuery table: {table_id}")

        # Load processed data into BigQuery
        load_to_bigquery(customers_df, "Customers")
        load_to_bigquery(products_df, "Products")
        load_to_bigquery(orders_df, "Orders")
        load_to_bigquery(orderDetails_df, "OrderDetails")

    except Exception as e:
        print(f"Error processing message: {str(e)}")
