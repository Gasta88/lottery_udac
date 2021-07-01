from pyspark.sql import SparkSession
import unittest
from etl import process_inputdata, create_customers_table, \
    create_websites_table, create_products_table, create_bookings_table, \
        create_logins_table, create_registrations_tables, \
            create_tickets_table, create_times_table

class ETLTest(unittest.TestCase):
    """Test suite for the PySpark ETL pipeline."""

    def setUp(self):
        """Initialize the test settings."""
        self.spark = SparkSession \
            .builder \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
            .getOrCreate()
        input_data = 'test_data'
        try:
            self.log_df
        except AttributeError:
            self.log_df, self.reg_df, self.lottery_df, self.games_df = \
                process_inputdata(self.spark, input_data)

    def tearDown(self):
        """Remove test settings."""
        self.spark.stop()

    def test_create_customers_table(self):
        """Test elt.create_customers_table method."""
        df = create_customers_table(self.spark, self.reg_df,
                                    debug=1)
        self.assertTrue(df.count() > 0)

    def test_create_websites_table(self):
        """Test elt.create_websites_table method."""
        df = create_websites_table(self.spark, self.log_df, self.reg_df,
                                    self.lottery_df, self.games_df, debug=1)
        self.assertTrue(df.count() > 0)

    def test_create_tickets_table(self):
        """Test etl.create_tickets_table method."""
        df = create_tickets_table(self.spark, self.lottery_df,
                                  self.games_df, debug=1)
        self.assertTrue(df.count() > 0)

    def test_create_products_table(self):
        """Test etl.create_products_table method."""
        df = create_products_table(self.spark, self.lottery_df,
                                    self.games_df, debug=1)
        self.assertTrue(df.count() > 0)

    def test_create_times_table(self):
        """Test etl.create_times_table method."""
        df = create_times_table(self.spark, self.log_df, self.reg_df,
                                self.lottery_df, self.games_df, debug=1)
        self.assertTrue(df.count() > 0)

    # def test_create_registrations_table(self):
    #     """Test etl.create_registrations_tables method."""
    #     df = create_registrations_tables(self.spark, self.reg_df, debug=1)
    #     self.assertTrue(df.count() > 0)

    # def test_create_logins_table(self):
    #     """Test etl.create_logins_table method."""
    #     df = create_logins_table(self.spark, self.log_df, debug=1)
    #     self.assertTrue(df.count() > 0)

    # def test_create_bookings_table(self):
    #     """Test etl.create_bookings_table method."""
    #     df = create_bookings_table(self.spark, self.lottery_df, self.games_df,
    #                                debug=1)
    #     self.assertTrue(df.count() > 0)

if __name__ == '__main__':
    runner = unittest.TextTestRunner(verbosity=3)
    unittest.main(testRunner=runner)