import configparser
import psycopg2


def drop_tables(cur, conn):
    registrations_table_drop = "DROP TABLE IF EXISTS registrations;"
    bookings_table_drop = "DROP TABLE IF EXISTS bookings;"
    logins_table_drop = "DROP TABLE IF EXISTS logins;"
    customers_table_drop = "DROP TABLE IF EXISTS customers;"
    times_table_drop = "DROP TABLE IF EXISTS times;"
    products_table_drop = "DROP TABLE IF EXISTS products;"
    tickets_table_drop = "DROP TABLE IF EXISTS tickets;"
    websites_table_drop = "DROP TABLE IF EXISTS websites;"
    queries = [registrations_table_drop,
               bookings_table_drop ,
               logins_table_drop,
               customers_table_drop,
               times_table_drop,
               products_table_drop,
               tickets_table_drop,
               websites_table_drop]
    for query in queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    customers_table_create = ("""CREATE TABLE IF NOT EXISTS customers(
                                                   id BIGINT PRIMARY KEY sortkey,      
                                                   email TEXT,
                                                   dateofbirth TEXT,
                                                   familyname TEXT,
                                                   givennames TEXT,
                                                   address TEXT,
                                                   city TEXT,
                                                   federalstate TEXT,
                                                   postalcode TEXT,
                                                   sovereignstate TEXT,
                                                   street TEXT);
                             """)
    websites_table_create = ("""CREATE TABLE IF NOT EXISTS websites(
                                                   id BIGINT PRIMARY KEY sortkey,      
                                                   name TEXT);
                             """)
    tickets_create_table = ("""CREATE TABLE IF NOT EXISTS tickets(
                                                  id TEXT PRIMARY KEY sortkey,
                                                  amount REAL,
                                                  fee REAL,
                                                  winnings REAL);
                            """)
    products_create_table = ("""CREATE TABLE IF NOT EXISTS products(
                                                  id BIGINT PRIMARY KEY sortkey,
                                                  name TEXT,
                                                  type TEXT);
                             """)
    times_create_table = ("""CREATE TABLE IF NOT EXISTS times(
                                            "timestamp" BIGINT PRIMARY KEY sortkey,
                                            hour INTEGER ,
                                            day INTEGER ,
                                            week INTEGER ,
                                            month INTEGER ,
                                            year INTEGER ,
                                            weekday TEXT ) diststyle all
                          """)
    registrations_create_table = ("""CREATE TABLE IF NOT EXISTS registrations(
                                            id BIGINT PRIMARY KEY,
                                            customernumber BIGINT NOT NULL REFERENCES customers(id) distkey,
                                            website_id BIGINT NOT NULL REFERENCES websites(id),
                                            "timestamp" BIGINT NOT NULL REFERENCES times("timestamp") sortkey);
                          """)
    logins_create_table = ("""CREATE TABLE IF NOT EXISTS logins(
                                            id BIGINT PRIMARY KEY,
                                            customernumber BIGINT NOT NULL REFERENCES customers(id) distkey,
                                            website_id BIGINT NOT NULL REFERENCES websites(id),
                                            "timestamp" BIGINT NOT NULL REFERENCES times("timestamp") sortkey);
                           """)
    bookings_create_table = ("""CREATE TABLE IF NOT EXISTS bookings(
                                            id BIGINT PRIMARY KEY,
                                            customernumber BIGINT NOT NULL REFERENCES customers(id) distkey,
                                            website_id BIGINT NOT NULL REFERENCES websites(id),
                                            "timestamp" BIGINT NOT NULL REFERENCES times("timestamp") sortkey,
                                            ticket_id TEXT NOT NULL REFERENCES tickets(id),
                                            product_id BIGINT NOT NULL REFERENCES products(id),
                                            currency TEXT,
                                            amount REAL);
                           """)
    queries = [customers_table_create, websites_table_create,
               tickets_create_table, products_create_table,
               times_create_table,
               registrations_create_table, logins_create_table,
               bookings_create_table]
    for query in queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('aws.cfg')
    
    DWH_DB = config.get("DWH","DB_NAME")
    DWH_HOST = config.get("DWH", "HOST")
    DWH_DB_USER = config.get("DWH","DB_USER")
    DWH_DB_PASSWORD = config.get("DWH","DB_PASSWORD")
    DWH_PORT = config.get("DWH","DB_PORT")
    
    conn = psycopg2.connect(f"host={DWH_HOST} dbname={DWH_DB} user={DWH_DB_USER} password={DWH_DB_PASSWORD} port={DWH_PORT}")
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()