## Imports

import pandas as pd
import numpy as np
import psycopg2

from pyspark import SparkConf
from pyspark.sql import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


class retail_pipe():
    """
    Class to initialize the pipeline for ingestion

    Process Flow:

    1.  User defines the filepath to the data and gives the database connection settings.

    2.  Data is first ingested via the spark API.

    3.  Data is then cleaned based on some rules listed below. An audit log is maintained of
        all the records rejected.

    4.  The cleaned data and the audit log are then written to the postgres database using JDBC drivers.

    5.  The aggregate with the composite key of "CustomerID", "InvoiceNo" is done on the "Quantity"

    6.  This aggregated data is then written to a temporary table in the database.

    7.  An Update or Insert query is performed on our persistent aggregate table. All the values
        of temporary table are used to perform this operation. Any new "CustomerID", "InvoiceNo" are inserted
        and existing have their "Quantity" updated accrodingly.
    
    8.  We perform an update on the "stock_info" (this table contains the information about the items being sold) 
        database table, such as their description and unit price. The values coming in from
        ingested data are either simply inserted if new to the database or are used to update 
        UnitPrice of the exisitng items as well as check on the description.

    9.  Once we have updated both the "stock_info" and "retail_agg" (The aggregate table in the database). We perform
        and INNER JOIN on "StockCode" on both these tables to get an output of a csv file for us to perform our visual
        analysis on.


    Tables:

    Retail_audit & retail_atomic

    Id      InvoiceNo   StockCode   Description     Quantity    InvoiceDate UnitPrice   CustomerID  Country
    Serial  Varchar(6)  Varchar(5)  Varchar(150)    Int         Timestamp   Numeric     Varchar(6)  Varchar(30)


    Retail_agg & retail_temp (the temporary table for updating)

    CustomerID  StockCode   Country         QuantitySum
    Varchar(6)  Varchar(5)  Varchar(150)    integer

    stock_info & stock_temp (the temporary table for updating)

    StockCode   Description    UnitPrice    Instances
    varchar(5)  varchar(150)   Numeric      Integer

    """
    def __init__(self, filepath,database,username,password):

        # Create the Spark Session, config with the classpath to postgres JDBC driver
        self.spark=SparkSession\
            .builder \
            .master('local') \
            .appName("Clairvoyant Project")\
            .config("spark.driver.extraClassPath","project_Clair\postgresql-42.2.23.jar")\
            .config("spark.pyspark.python","Scripts\python.exe")\
            .getOrCreate()

        print('-Spark Session Configuration-\n'+'\n'.join(map(str, SparkConf().getAll()))) 
        self.data = self.spark.read.csv(filepath, header=True, inferSchema=True)
        
        print('Data imported from the file successfully! \n')
        print(self.data.show(10))
        print(self.data.printSchema())

        ## Database Connection Settings
        self.database=database
        self.username=username
        self.password=password



    def data_processing(self):
        """
        DATA CLEANING

        InvoiceDate is converted to timestamp.type

        Following checks are done on the data:

            · Invoice number should be integer and length 6
            
            · Stock Code should be integer and length 5

            · Quantity should be integer

            · Invoice Date has to be timestamp.type

            · UnitPrice is Double
            
            · CustomerID should be integer of length 5

        No Null Values allowed in the final dataset

        All Duplicates are removed from the table

        Audit log is maintained for values not passing the checks.
        """
        # Missing Data counts
        print('-Missing Values Count- \n')
        print(self.data.select([count(when(col(c).isNull(), c)).alias(c) for c in self.data.columns]).show())


        # Convert the InvoiceDate Column to timestamp type

        self.data=self.data.withColumn('InvoiceDate',to_timestamp('InvoiceDate',"dd-MM-yyyy HH:mm"))

        # The clean data will go into data_clean

        self.data_clean= self.data.filter(
                    col("StockCode").cast("int").isNotNull() & (length(col("StockCode").cast(IntegerType()).cast(StringType()))==5) &
                    col("InvoiceNo").cast('int').isNotNull() & (length(col('InvoiceNo').cast(IntegerType()).cast(StringType()))==6) &
                    col('Quantity').cast('int').isNotNull() &
                    col('UnitPrice').cast(DoubleType()).isNotNull() &
                    col('CustomerID').cast('int').isNotNull() & (length(col('CustomerID').cast(IntegerType()).cast(StringType()))==5) &
                    col('InvoiceDate').isNotNull() &
                    col('Description').isNotNull()
                    )

        # Converting the CustomerID to string as its a nominal variable and to have consistency
        # in the dataset when dealing with Nominal variables.
        self.data_clean=self.data_clean.withColumn('CustomerID',col('CustomerID').cast(IntegerType()).cast(StringType()))
        self.data_clean=self.data_clean.withColumn('StockCode',col('StockCode').cast(IntegerType()).cast(StringType()))
        self.data_clean=self.data_clean.withColumn('InvoiceNo',col('InvoiceNo').cast(IntegerType()).cast(StringType()))

        # Drop the Duplicates
        self.ata_clean=self.data_clean.dropDuplicates()

        # All the rejected values go into the data_audit

        self.data_audit =self.data.filter(~(
                    col("StockCode").cast("int").isNotNull() & (length(col("StockCode").cast(IntegerType()).cast(StringType()))==5) &
                    col("InvoiceNo").cast('int').isNotNull() & (length(col('InvoiceNo').cast(IntegerType()).cast(StringType()))==6) &
                    col('Quantity').cast('int').isNotNull() &
                    col('UnitPrice').cast(DoubleType()).isNotNull() &
                    col('CustomerID').cast('int').isNotNull() & (length(col('CustomerID').cast(IntegerType()).cast(StringType()))==5) &
                    col('InvoiceDate').isNotNull() &
                    col('Description').isNotNull())
                    )


        print('Data Cleaned and Audit log created \n')

        self.stock_update()

    def stock_update(self):
        
        print("Updating the stock data table with the incoming data.")

        # get the destinct descriptions of items from the database
        self.descrip=self.data_clean.select('StockCode',"Description").distinct()\
                                    .groupBy("StockCode").agg(max("Description").alias("Description"))
        
        # print(self.descrip.collect())
        
        # average the unitprice for all the identical items in the database
        self.uprice=self.data_clean.select('StockCode',"UnitPrice").distinct()\
                                    .groupBy("StockCode").agg(avg("UnitPrice").alias("UnitPrice"), count("UnitPrice").alias("Instances"))

        self.stock_info=self.uprice.join(self.descrip,self.uprice.StockCode==self.descrip.StockCode, "inner")\
                                    .select(self.descrip.StockCode,"Description","UnitPrice","Instances")

        self.stock_info.write \
            .format("jdbc") \
            .mode("overwrite") \
            .option("url","jdbc:postgresql://localhost:5432/"+self.database)\
            .option("dbtable", "public.stock_temp") \
            .option("user", self.username) \
            .option("password", self.password) \
            .option("driver", "org.postgresql.Driver") \
            .save()
        
        """
        QUERY TO UPDATE THE STOCK ITEM INFORMATION IN THE DATABASE.

        INSERT INTO stock_info as si ("StockCode","Description","UnitPrice","Instances")
            SELECT t."StockCode", t."Description", t."UnitPrice", t."Instances"
                FROM stock_info as s
                RIGHT JOIN stock_temp as t
                    ON t."StockCode"=s."StockCode"
        ON CONFLICT ON CONSTRAINT stockkey DO UPDATE SET
            "UnitPrice"= (si."UnitPrice"*si."Instances" + EXCLUDED."UnitPrice" * EXCLUDED."Instances")/ (si."Instances" + EXCLUDED."Instances"),
            "Instances"= si."Instances" + EXCLUDED."Instances",
            "Description"= CASE 
                WHEN char_length(si."Description")>=char_length(EXCLUDED."Description") THEN si."Description"
                ELSE EXCLUDED."Description" END;
;
        """

        try:
            # Call to write the aggregating data and other tables ( Audit and Atomic) into the database
            self.database_write()
            
            # Query to update stock items
            upsert_query="INSERT INTO stock_info as si (\"StockCode\",\"Description\",\"UnitPrice\",\"Instances\")\
                            SELECT t.\"StockCode\", t.\"Description\", t.\"UnitPrice\", t.\"Instances\"\
                                FROM stock_info as s\
                                RIGHT JOIN stock_temp as t\
                                ON t.\"StockCode\"=s.\"StockCode\"\
                        ON CONFLICT ON CONSTRAINT stockkey DO UPDATE SET\
                            \"UnitPrice\"= (si.\"UnitPrice\"*si.\"Instances\" + EXCLUDED.\"UnitPrice\" * EXCLUDED.\"Instances\")/ (si.\"Instances\" + EXCLUDED.\"Instances\"),\
                            \"Instances\"= si.\"Instances\" + EXCLUDED.\"Instances\",\
                            \"Description\"= CASE \
                                             WHEN char_length(si.\"Description\")>=char_length(EXCLUDED.\"Description\") THEN si.\"Description\"\
                                             ELSE EXCLUDED.\"Description\" END"

            print("Connecting to the PostgreSQL server for the upsert in the stock_info table....")
            conn = psycopg2.connect(
                host="localhost",
                database=self.database,
                user=self.username,
                password=self.password,
                port="5432")

            cursor=conn.cursor()

            cursor.execute(upsert_query)

            conn.commit()

            try:
                # Call to export the aggregated data
                self.export_aggdata()
            except Exception as e:
                print("Failed to export the aggregate data. \n", e)


        except (Exception, psycopg2.Error) as error:
            print("Failed to update records in stock_info. Database operations cancelled. \n ", error)

        finally:
            if conn:
                cursor.close()
                conn.close()
                print("PostgreSQL connection is closed.")


    def database_write(self):
        """ 
        Append the clean and audit log to the database and
        Upsert the aggregate data table to the database.

        Aggregate data using CustomerID and StockCode.

        Some of the StockCode have different unit price, so we average them all in the end.

        Some of the Descriptions are different from each other so take the largest description 
        from the resulting group and same stockcode descriptions.
        """
        print(self.data_clean.count(),self.data_audit.count())
        
        ## Temporary aggregate data from the new file coming in
        self.temp_agg=self.data_clean.groupBy('CustomerID','StockCode','Country')\
                   .agg(sum("Quantity").alias("QuantitySum"))
        
        print("Writing the aggregate data to the temporary aggregate database in postgres \n")
        
        ## Overwriting the aggregated data into a temporary table in the postgres database
        self.temp_agg.write \
            .format("jdbc") \
            .mode("overwrite") \
            .option("url","jdbc:postgresql://localhost:5432/"+self.database)\
            .option("dbtable", "public.retail_agg_temp") \
            .option("user", self.username) \
            .option("password", self.password) \
            .option("driver", "org.postgresql.Driver") \
            .save()

        '''
        QUERY FOR Update AND Insert (UPSERT) THE AGGREGATE DATA IN THE DATABASE

        INSERT INTO retail_agg as ra ("CustomerID", "StockCode", "Country", "QuantitySum")
            SELECT t."CustomerID", t."StockCode", t."Country", t."QuantitySum"
                FROM retail_agg as r
                RIGHT JOIN retail_agg_temp as t 
                ON t."CustomerID"=r."CustomerID" AND t."StockCode"=r."StockCode" AND t."Country"=r."Country" 
        ON CONFLICT ON CONSTRAINT aggkey
        DO UPDATE SET  
            "QuantitySum" = ra."QuantitySum"+EXCLUDED."QuantitySum";

        '''
        ### Upsert in the database with the temp table we have from the new data

        try:
            upsert_query="INSERT INTO retail_agg as ra (\"CustomerID\", \"StockCode\", \"Country\", \"QuantitySum\")SELECT t.\"CustomerID\", t.\"StockCode\", t.\"Country\", t.\"QuantitySum\" FROM retail_agg as r RIGHT JOIN retail_agg_temp as t ON t.\"CustomerID\"=r.\"CustomerID\" AND t.\"StockCode\"=r.\"StockCode\" AND t.\"Country\"=r.\"Country\" ON CONFLICT ON CONSTRAINT aggkey DO UPDATE SET \"QuantitySum\" = ra.\"QuantitySum\"+EXCLUDED.\"QuantitySum\""

            print("Connecting to the PostgreSQL server for the upsert in the aggregate table....")
            conn = psycopg2.connect(
                host="localhost",
                database=self.database,
                user=self.username,
                password=self.password,
                port="5432")

            cursor=conn.cursor()

            cursor.execute(upsert_query)

            conn.commit()

            print("Writing the Clean Atomic Data and Audit Log to database \n")
            ## Appending the clean ATOMIC DATA to the postgres database
            self.data_clean.write \
                .format("jdbc") \
                .mode("append") \
                .option("url","jdbc:postgresql://localhost:5432/"+self.database)\
                .option("dbtable", "public.retail_atomic") \
                .option("user", self.username) \
                .option("password", self.password) \
                .option("driver", "org.postgresql.Driver") \
                .save()


            ## Appending the AUDIT DATA to the postgres database
            self.data_audit.write \
                .format("jdbc") \
                .mode("append") \
                .option("url","jdbc:postgresql://localhost:5432/"+self.database)\
                .option("dbtable", "public.retail_audit") \
                .option("user", self.username) \
                .option("password", self.password) \
                .option("driver", "org.postgresql.Driver") \
                .save()
            
            

        except (Exception, psycopg2.Error) as error:
            print("Failed to update records in retail_agg from the retail_agg_temp. Database operations cancelled. \n ", error)

        finally:
            if conn:
                cursor.close()
                conn.close()
                print("PostgreSQL connection is closed.")


    def export_aggdata(self):
        """Export the aggregate data to a csv file"""

        ### Read the new updated aggregate data into a PySpark Database
        print("Reading the aggregate data from the database to export as CSV file \"Retail Aggregate.csv\" \n")
        
        """
        QUERY USED TO EXTRACT THE EXPORT DATA FROM THE DATABASE
        
        SELECT a."CustomerID", a."StockCode", s."Description", a."Country", s."UnitPrice", a."QuantitySum"
        FROM retail_agg a INNER JOIN stock_info s ON a."StockCode"=s."StockCode";
        """
        retrieve_query="(SELECT a.\"CustomerID\", a.\"StockCode\", s.\"Description\", a.\"Country\", s.\"UnitPrice\", a.\"QuantitySum\"\
             FROM retail_agg a INNER JOIN stock_info s ON a.\"StockCode\"=s.\"StockCode\") as aggdata"

        self.data_agg=self.spark.read \
                .format("jdbc") \
                .option("url","jdbc:postgresql://localhost:5432/"+self.database)\
                .option("dbtable", retrieve_query) \
                .option("user", self.username) \
                .option("password", self.password) \
                .option("driver", "org.postgresql.Driver") \
                .load()

        self.data_agg.toPandas().to_csv('Retail Aggregate.csv', index=False)



if __name__ == "__main__":
    # Filepath to the data to be processed
    filepath="Online Retail.csv"

    database="postgres"
    username="postgres"
    password="password"
    
    
    retail=retail_pipe(filepath,database,username,password)
    retail.data_processing()
    # retail.database_write()
