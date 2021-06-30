from datetime import datetime
import time
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id, lit, \
    concat, lower
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, \
    date_format
from pyspark.sql.types import IntegerType, StringType, TimestampType
from functools import reduce
import unittest
from ..etl import process_inputdata, create_customers_table, \
    create_websites_table, create_products_table, create_bookings_table, \
        create_logins_table, create_registrations_tables, \
            create_tickets_table, create_times_table

class ETLTest(unittest.TestCase):
    """Test suite for the PySpark ETL pipeline."""

    def setUp(self):
        """Initialize the test settings."""
        pass

    def tearDown(self):
        """Remove test settings."""
        pass

    def test_create_customers_table(self):
        """Test elt.create_customers_table method."""
        pass

    def test_create_websites_table(self):
        """Test elt.create_websites_table method."""
        pass

    def test_create_tickets_table(self):
        """Test etl.create_tickets_table method."""
        pass

    def test_create_products_table(self):
        """Test etl.create_products_table method."""
        pass

    def test_create_times_table(self):
        """Test etl.create_times_table method."""
        pass

    def test_create_registrations_table(self):
        """Test etl.create_registrations_tables method."""
        pass

    def test_create_logins_table(self):
        """Test etl.create_logins_table method."""
        pass

    def test_create_bookings_table(self):
        """Test etl.create_bookings_table method."""
        pass

if __name__ == '__main__':
    runner = unittest.TextTestRunner(verbosity=3)
    unittest.main(testRunner=runner)