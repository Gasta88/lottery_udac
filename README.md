# Lottery data - Udacity Data Engineering Capstone Project

## Purpose/Goals

The company Lotto24 is an online provider of state licensed lotteries. From several servers, they log user activities (access and registration) and purchases of different items. 

For this case study, only lottery tickets and instant games are considered.

The objective of this project is to create a data warehouse where a star schema can be hosted for analytical investigations, along with a small data visualization notebook to run some queries on the database.

The data warehouse is implemented in [AWS Redshift](https://docs.aws.amazon.com/redshift/latest/mgmt/welcome.html) , while the data is manipulated via [Apache Spark](https://spark.apache.org/) run on an [AWS EMR](https://aws.amazon.com/emr/?whats-new-cards.sort-by=item.additionalFields.postDateTime&whats-new-cards.sort-order=desc) cluster.

The data visualization app is implemented in [Jupyter](https://jupyter.org/) notebook and run the following queries:

    - Display information about the total billing groupped by years.
    - Display information about the total billing groupped by years/months.
    - Display information about the total number of items sold by type/years/month.
    - Display information about the total number of active access by year/month and website. For this example only 5 websites have been picked.
    - Display information about the total number of registrations by weekday and website. For this example only 5 websites have been picked.
    - Display information about the monthly difference billing by year/month.
    
AWS Redshift is a highly scalable cloud database solution that can handle high data volume without too many complains. It has been chosen because it has a familiar usage to Postgres and the cloud implementation helps to scale up/down resources, while keeping it available for the entire user-base.

[PySpark](https://spark.apache.org/docs/latest/api/python/) is the Python implementation of the Apache Spark framework. It has been associated with AWS EMR because it is easier to setup and it requires minimal
configuration for one or many clusters. It comes with Apache Spark and several other useful applications, which can be customizable via AWS Console. The key aspect is great availability of RAM to store the data during its processing. It is the perfect choice for big data pipelines.

The visualization is done in Jupyter notebook using [Pandas](https://pandas.pydata.org/) for easy data manipulation and [ipython-sql](https://pypi.org/project/ipython-sql/) for querying the data warehouse directly.

## Schema design

![image info](./imgs/imgs/multi-START.png)

### Main design

The database `lotterydb` holds information about bookings and user events, such as registrations and logins. 

The star schema is made of 3 fact tables and 5 dimension tables.

   - *fact tables*: `bookings`, `logins` and `registrations`
   - *dimension tables*: `customers`, `websites`, `times`, `products` and `tickets`


### Data assumptions

Some assumptions have been taken into account and they are investigated deeply on the Jupyter notebook located here: `notebooks/EDA.ipynb`.

On the ELT, the `customers` table must not have missing values in the following columns: *customernumber*, *customeremail*, *givennames*, *familynames*,
 *timestamp* and *dateofbirth*.
 Moreover the *customeremail* must have a valid format for email addresses.

For the `websites` table, only the website name coming from different sources should not be missing.

For the `tickets` table the following columns are considered necessary: *id* (the format changes from lottery tickets to instant game tickets), *timestamp*,
*feeineur*, *priceineur*, *website*, *customernumber* and *gamename*.

For the `products` table, the name of the game (converted in lowercase) must not be empty.

For the `times` table, the *timestamp* column retrieved from several datasources must be available. 



Running the notebook `EDA.ipynb` it is possible to see that there are some inconsistency in the raw data. In this project, records that do not conform with the QA standards will
be simply ignored.
In other scenarios, the problematic records could be stored in `quarantine` set of tables used for manual inspections. This would provide the following advantages:

    - records are not simply discarded and everything is kept for possible future updates and corrections
    - records can be used for a cross-departament investigation to spot out problems that have passed unobserved (ex: implementation of some access points is faulty, some useful data is lost while it is expected to be captured, etc)
    - records won't compromise the ETL workflow when an error is discovered. Records fit for the OLAP section of the data warehouse can still be correctly used.


### Initial data processing

At the early stage of the ETL, the data has been processed at the source to make the different datasets more comparable between each others.
The development of UDF made possible the generation of different time columns:
    - timestamp in UNIX format
    - datetime in human readable format
    
The lottery games required a conversion of their fees and payment into EUR  from CENTS. Some other sources (like instant games and lottery) required empty columns to successfully join datasets in a later time.


## Alternative scenarios

1. *What if the data was increased by 100x?*
AWS Redshift already represents a good choice when we are talking about big data problems. Thanks to its cloud implementation, it is possible to scale both vertically and horizontally
the cluster system to hold more data. The difference is dictated by cost and the necessity of having a highly available system. Alternatives that can be considered are:
    - local installation of Hadoop into a on-premises solution
    - Snowflake
    - Google or Microsoft counterparts
    
2. *What if the pipelines were run on a daily basis by 7am?*
The least recommended method to schedule your pipeline is to set it up onto a server and configure a CRON/Task Scheduler job, depending on the server OS.
Ideally it is best to use a data orchestration solution like Apache Airflow, Luigi or DAGster. Such framework can schedule your pipelines more effectively but also bring more advantages such as:
    - improved data lineage
    - better data quality
    - maximize parallelism
    - make failure states obvious
    - improve maintainability
    
3. *What if the database needed to be accessed by 100+ people?*
AWS Redshift can  be made more accessible when it is scaled horizontally (ex: more nodes with low/med specs are available). Managing these numbers is quite intuitive on the AWS Console.
    
 
## How to run the project

### Prerequisites

Before running the project, the user must satisfy these requirements:
 
- Python v3.6+
- Docker v20+
- Git v2.30+

### Run the workflow

Once the repository is cloned locally, follow these steps:

1. Create an AWS Redshift cluster via AWS Console with the following specs:
    a. *free-tier* resources
    b. public available
    
2. Create an EMR cluster via AWS Console with the following specs:
    a. release *emr-5.20.0*
    b. 3 nodes (1 Master and 2 Slaves)
    c. make sure to have a paired key stored locally. Use that for security access.
    d. if the cluster is created via different machines, make sure to log onto the security group for the Master node and change the IP for the SSH inbound rule.
    
3. Fill out the missing information from the `aws.cfg` file.

4. **OPTIONAL**: Run the `notebooks/EDA.ipynb` to inspect QA issues in the data.

5. Connect to the AWS EMR cluster via PuTTY or SSH command (always specify the paired key).
   SCP `aws.cfg`, `RedshiftJDBC4-no-awssdk-1.2.20.1043.jar` and `etl.py` onto the cluster. I personally use [WinSCP](https://winscp.net/eng/download.php)
   
6. The source data is already stored onto a S3 bucket: `udac-lottery-data`.

7. On the local directory run `python create_tables.py` to create the database schema.

8. After that run the command on the SHH terminal: `spark-submit --jars RedshiftJDBC4-no-awssdk-1.2.20.1043.jar --packages com.databricks:spark-redshift_2.10:2.0.0,org.apache.spark:spark-avro_2.11:2.4.0,com.eclipsesource.minimal-json:minimal-json:0.9.4 etl.py 1> spark.log` to run the pipeline on the EMR SSH console.
 It will collect the STDOUT into the `spark.log` file where it will be easier to debug any ETL issue on the fly.
 
9. When the ETL is completed, terminate the AWS EMR cluster via AWS Console.
 
10. Run the `notebooks/data_visualization.ipynb` to inspect the default queries in the data warehouse.
 
## Files in the repo

Important files in the repository are:

- `create_tables.py`: creates empty table structures on AWS Redshift. If they exist already, these are dropped from the schema.
- `etl.py`: main ETL script.
- `RedshiftJDBC4-no-awssdk-1.2.20.1043.jar`: AWS Redshift JDBC driver to connect PySpark to the database for read/write operations.
- `imgs`: contains the PNG of the star schema.
- `aws.cfg`: contains configuration parameters for AWS.
