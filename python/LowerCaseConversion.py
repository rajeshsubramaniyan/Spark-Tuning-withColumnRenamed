import datetime
from pyspark.sql import SparkSession

# Initialize SparkSession
def spark_init(app_name):
    spark = SparkSession.builder.appName(app_name) \
        .master("local[*]") \
        .config("spark.logConf", "true") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

# Initialize Logging
def spark_logger(set_spark_ses):
    log4jLogger = set_spark_ses._jvm.org.apache.log4j
    log = log4jLogger.LogManager.getLogger(__name__)
    log.warn("***************************Logging Initiated***************************")
    return log

def perform_case_conversion(spark_ses, spark_log):
    # Load input csv file
    inputDF = spark_ses.read.csv("C:\\Spark\\Data_100_Columns.csv", header=True)
    print("Columns before case conversion..\n")
    inputDF.show(truncate=False)

    print ("Converting using withColumnRenamed.. \n")
    time_stamp = datetime.datetime.now()
    print(str(time_stamp) + " - Lower case conversion started! \n")

    for col_name in inputDF.columns:
        inputDF = inputDF.withColumnRenamed(col_name, col_name.lower())

    time_stamp = datetime.datetime.now()
    print(str(time_stamp) + " - Lower case conversion finished! \n")

    inputDF.show(truncate=False)

    print ("Converting using toDF.. \n")
    time_stamp = datetime.datetime.now()
    print(str(time_stamp) + " - Lower case conversion started! \n")

    inputDF = inputDF.toDF(*[column.lower() for column in inputDF.columns])

    time_stamp = datetime.datetime.now()
    print(str(time_stamp) + " - Lower case conversion finished! \n")

    inputDF.show(truncate=False)

if __name__ == '__main__':
    print ("Spark Application for lower case conversion is getting started..")
    get_spark_ses = spark_init("lower_case_conversion")
    get_logger = spark_logger(get_spark_ses)
    perform_case_conversion(get_spark_ses, get_logger)
    print ("Spark application for lower case conversion finished..")