# Databricks notebook source
# MAGIC %run ./invoice_application

# COMMAND ----------

class invoiceStreamTestSuite():
    def __init__(self):
        self.base_data_dir = "/FileStore/invoices"

    def cleanTests(self):
        print(f"Starting Cleanup...", end='')
        spark.sql("drop table if exists invoice_line_items")
        dbutils.fs.rm("/user/hive/warehouse/invoice_line_items", True)

        dbutils.fs.rm(f"{self.base_data_dir}/checkpoint/invoices", True)
        dbutils.fs.rm(f"{self.base_data_dir}/datasets/invoices", True)

        dbutils.fs.mkdirs(f"{self.base_data_dir}/datasets/invoices")
        print("Done")

    def ingestData(self, itr):
        print(f"\tStarting Ingestion...", end='')
        dbutils.fs.cp(f"{self.base_data_dir}/data/invoices_{itr}.json", f"{self.base_data_dir}/datasets/invoices/")
        print("Done")

    def assertResult(self, expected_count):
        print(f"\tStarting validation...", end='')
        actual_count = spark.sql("select count(*) from invoice_line_items").collect()[0][0]
        assert expected_count == actual_count, f"Test failed! actual count is {actual_count}"
        print("Done")

    def waitForMicroBatch(self, sleep=30):
        import time
        print(f"\tWaiting for {sleep} seconds...", end='')
        time.sleep(sleep)
        print("Done.")

    def runTests(self):
        self.cleanTests()
        iStream = invoiceWC()
        streamQuery = iStream.process()

        print("Testing first iteration of invoice stream...") 
        self.ingestData(1)
        self.waitForMicroBatch()        
        self.assertResult(1249)
        print("Validation passed.\n")

        print("Testing second iteration of invoice stream...") 
        self.ingestData(2)
        self.waitForMicroBatch()
        self.assertResult(2506)
        print("Validation passed.\n") 

        print("Testing third iteration of invoice stream...") 
        self.ingestData(3)
        self.waitForMicroBatch()
        self.assertResult(3990)
        print("Validation passed.\n")

        streamQuery.stop()


# COMMAND ----------

isTS = invoiceStreamTestSuite()
isTS.runTests()	

# COMMAND ----------


