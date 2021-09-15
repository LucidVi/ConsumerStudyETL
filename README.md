# Consumer Study 

Dashboard:

   https://public.tableau.com/app/profile/vishu.gupta/viz/TestClair/Dashboard1

Data Used:

Online Retail Dataset
    https://archive.ics.uci.edu/ml/datasets/online+retail#

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

Technologies Used:

    1. PySpark
    2. PostgreSQL
    3. Pandas
    
