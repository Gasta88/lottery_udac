from pyspark.sql import SparkSession
import unittest
import os
import warnings
from pyspark.sql.functions import col
from etl import process_inputdata, create_customers_table, \
    create_websites_table, create_products_table, create_bookings_table, \
        create_logins_table, create_registrations_tables, \
            create_tickets_table, create_times_table

class ETLTest(unittest.TestCase):
    """Test suite for the PySpark ETL pipeline."""

    def setUp(self):
        """Initialize the test settings."""
        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)
        self.spark = SparkSession \
            .builder \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel('ERROR')
        self.input_data = 'tests/test_data'
        try:
            self.log_df
        except AttributeError:
            self.log_df, self.reg_df, self.lottery_df, self.games_df = \
                process_inputdata(self.spark, self.input_data)

    def tearDown(self):
        """Remove test settings."""
        self.spark.stop()

    def test_create_customers_table(self):
        """Test elt.create_customers_table method."""
        df = create_customers_table(self.spark, self.reg_df,
                                    debug=1)
        n_rows = df.count()

        self.assertTrue(df.count() > 0)
        self.assertTrue(df.where(col('customeremail').isNotNull()).count() == \
                        n_rows)
        self.assertTrue(df.where(col('dateofbirth').isNotNull()).count() == \
                        n_rows)
        self.assertTrue(df.where(col('givennames').isNotNull()).count() == \
                        n_rows)
        self.assertTrue(df.where(col('familyname').isNotNull()).count() == \
                        n_rows)
        self.assertTrue(df.where(col('timestamp').isNotNull()).count() == \
                        n_rows)
        self.assertTrue(df.where(col("customeremail")\
                                  .rlike('^[a-z0-9]+[\._]?[a-z0-9]+[@]\w+[.]\w{2,3}$'))\
                          .count() == n_rows)

    def test_create_websites_table(self):
        """Test elt.create_websites_table method."""
        df = create_websites_table(self.spark, self.log_df, self.reg_df,
                                    self.lottery_df, self.games_df, debug=1)
        n_rows = df.count()
        self.assertTrue(df.where(col('name').isNotNull()).count() == \
                        n_rows)
        duplicate_df = df.groupBy("name").count().filter('count > 1')
        self.assertTrue(duplicate_df.count() == 0)

    def test_create_tickets_table(self):
        """Test etl.create_tickets_table method."""
        df = create_tickets_table(self.spark, self.lottery_df,
                                  self.games_df, debug=1)
        duplicate_df = df.groupBy("id").count().filter('count > 1')
        self.assertTrue(duplicate_df.count() == 0)
        
        n_rows = df.count()
        self.assertTrue(df.where(col('fee').isNotNull()).count() == \
                        n_rows)
        self.assertTrue(df.where(col('amount').isNotNull()).count() == \
                    n_rows)

    def test_create_products_table(self):
        """Test etl.create_products_table method."""
        df = create_products_table(self.spark, self.lottery_df,
                                    self.games_df, debug=1)
        duplicate_df = df.groupBy("id").count().filter('count > 1')
        self.assertTrue(duplicate_df.count() == 0)
        self.assertTrue(df.groupBy("type").count().select("type").count() == 2)
        n_rows = df.count()
        self.assertTrue(df.where(col('name').isNotNull()).count() == \
                        n_rows)

    def test_create_times_table(self):
        """Test etl.create_times_table method."""
        df = create_times_table(self.spark, self.log_df, self.reg_df,
                                self.lottery_df, self.games_df, debug=1)
        n_rows = df.count()
        self.assertTrue(df.where(col('timestamp').isNotNull()).count() == \
                        n_rows)
        weekdays_list = list(df.select("weekday")\
                                .dropDuplicates(["weekday"])\
                                .toPandas()['weekday'])
        self.assertCountEqual(weekdays_list,['Sun', 'Mon', 'Thu', 'Sat',
                                                'Wed', 'Tue', 'Fri'])

    def test_create_registrations_table(self):
        """Test etl.create_registrations_tables method."""
        website_df = create_websites_table(self.spark, self.log_df, self.reg_df,
                                            self.lottery_df, self.games_df,
                                            debug=1)
        df = create_registrations_tables(self.spark, self.reg_df, debug=1,
                                          website_df=website_df)
        n_rows = df.count()
        self.assertTrue(df.where(col('timestamp').isNotNull()).count() == \
                        n_rows)
        webid_list = list(df.select("website_id")\
                            .dropDuplicates(["website_id"])\
                            .toPandas()['website_id'])
        self.assertEqual(len(webid_list), 10)
        

    def test_create_logins_table(self):
        """Test etl.create_logins_table method."""
        website_df = create_websites_table(self.spark, self.log_df, self.reg_df,
                                          self.lottery_df, self.games_df,
                                          debug=1)
        df = create_logins_table(self.spark, self.log_df, debug=1,
                                          website_df=website_df)
        n_rows = df.count()
        self.assertTrue(df.where(col('timestamp').isNotNull()).count() == \
                        n_rows)
        webid_list = list(df.select("website_id")\
                            .dropDuplicates(["website_id"])\
                            .toPandas()['website_id'])
        self.assertEqual(len(webid_list), 10)

    def test_create_bookings_table(self):
        """Test etl.create_bookings_table method."""
        website_df = create_websites_table(self.spark, self.log_df, self.reg_df,
                                          self.lottery_df, self.games_df,
                                          debug=1)
        product_df = create_products_table(self.spark, self.lottery_df,
                                           self.games_df, debug=1)
        df = create_bookings_table(self.spark, self.lottery_df, self.games_df,
                                    debug=1, website_df=website_df,
                                    product_df=product_df)
        n_rows = df.count()
        self.assertTrue(df.where(col('timestamp').isNotNull()).count() == \
                        n_rows)
        webid_list = list(df.select("website_id")\
                            .dropDuplicates(["website_id"])\
                            .toPandas()['website_id'])
        self.assertEqual(len(webid_list), 10)
        ticketid_list = list(df.select("ticket_id")\
                            .dropDuplicates(["ticket_id"])\
                            .toPandas()['ticket_id'])
        self.assertCountEqual(ticketid_list,
                              ['SAG920800728', 'FEV60315', 'ZIH240721',
                               'VEZ23430724', '93450', 'GOV70716',
                               'WAT766870703', '15488', '47202', '86600',
                               'ZOC060150', 'JUB546840128', '20352', '86619',
                               '98890', 'QIJ011990724', '27555', 'LOB0136',
                               '44379', 'VOF30610326', '84373', '82866',
                               '20149', '93454', '41816', 'LUY8480313',
                               'KON852150327', 'YOK120176', 'VOH22620719',
                               'VEW047050313', 'NUT10334', 'LAC779810150',
                               'CAB14390701', '32865', 'SON733080120',
                               'FAW77670324', 'XAB78730151', 'WOZ25600135',
                               '36149', '07969', 'PEY0152', 'RAQ850719',
                               'HAS0120', 'GUM20330720', '77224', '40239',
                               '57158', 'YOZ0148', 'ZEX0148', 'KIJ311190314',
                               '23725', '05521', '25674', '65405',
                               'TAH5790313', 'QOF80560142', 'LOX7040131',
                               '64828', '77522', '86782', '73171',
                               '60199', 'MAK2510314', 'RED440139',
                               'DAF508720160', 'LIW650727', 'ZEV530718',
                               'QIB730721', 'QOW211990701', 'TID45090716',
                               '58861', '40860', 'TEH0314', '19401', '56019',
                               '99939', '55463', '34914', 'XAW0162', 'HIN0150',
                               'KOP53440148', '73701', 'CEV19670725', '59110',
                               '83972', '23851', '39143', 'GEH930142', '28975',
                               'HIY92420724', '86935', '77714', '03246',
                               '67052', 'HOH90315', '83641', '01019', '77905',
                               '58473', 'RAW0725', 'SOC50724', '50749', '29870',
                               '90621', 'YOY900711', '73714', '18638', '97558',
                               'REL330129', '99563', '43976', '17010', '27968',
                               'YIL426190711', 'KUC33650160', 'LAK20120', 'ZAR0327',
                               'GAS190154', '34771', '33450', 'NEX250719',
                               'HUC493020148', '97298', '94242', 'RAV0160',
                               'JEJ0370151', 'MOM862690727', 'FAG550153',
                               'MIJ00151', '64654', 'JAF0161', '59902', '53479',
                               '32911', '37708', 'YOJ020716', 'BIT9770720',
                               'FIH0148', '50707', 'SUS60701', '74591', '43269',
                               '73583', '08736', 'COM49190161', '82964', 'KOZ53780138',
                               'XED0153', 'ZUR9320314', 'KAR50157', 'XUJ4340152',
                               'WIH80130', 'KEC0175', 'XEJ4400124', 'GOX510137',
                               '99379', '67893', '15678', '83714', 'XIY12110701',
                               '69228', 'BIS0144', 'REF48430703', '02341',
                               '86763', 'VAX0720', 'SIH50530725', 'CAJ25360335',
                               '71229', '26784', '63671', '88775', '28128',
                               '38389', '51194', 'PAG0325', 'LEW3280314',
                               'WOT0721', '17725', 'KAM6450709', 'REJ5290144',
                               '97357', '97975', 'MUW580709', 'BUN3310151',
                               'FAX070725', 'QAL5400125'])

if __name__ == '__main__':
    runner = unittest.TextTestRunner(verbosity=3)
    unittest.main(testRunner=runner)