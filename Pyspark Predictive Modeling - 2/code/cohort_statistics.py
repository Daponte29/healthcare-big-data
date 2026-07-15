import pyspark
import time
import pip
import pandas as pd
import numpy as np
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import datediff, to_date, max as max_, lit, col, collect_list, row_number, concat_ws, format_number, concat, monotonically_increasing_id
import random
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

   
def read_csv(spark, file, schema):
    return spark.read.csv(file, header=False, schema=schema)

def split_alive_dead(events, mortality):
    '''
    param: spart dataframe events: [petientid, eventid, etimestamp, value] and dataframe mortality: [patientid, mtimestamp, label]
    return: spark dataframe alive_evnets and dead_events

    Task1: This function needs to be completed.
    Split the events to two spark dataframes. One is for alive patients, and one is 
    for dead patients.
    Variables returned from this function are passed as input DataFrame for later.
    '''
    deceased_patient_ids = mortality.select("patientid").distinct()

    # Create alive and deceased Spark DataFrames
    alive_events = events.join(deceased_patient_ids, "patientid", "left_anti")
    dead_events = events.join(deceased_patient_ids, "patientid", "inner")
    
    
    
    
    
    
    
    return alive_events, dead_events

def event_count_metrics(alive_events, dead_events):
    '''
    param: two spark DataFrame: alive_events, dead_events
    return: two spark DataFrame

    Task 2: Event count metrics
    Compute average, min and max of event counts 
    for alive and dead patients respectively  
    +------+------+------+                                                   
    |   avg|   min|  max |
    +------+------+------+
    |value1|value2|value3|
    +------+------+------+
    note: 
    1.please keep same column name as example showed before!
    2.return two DataFrame for alive and dead patients' events respectively.
    3.average computed with avg(), DO NOT round the results.
    '''
    # Compute event counts for alive patients
    alive_count_events = alive_events.groupBy("patientid").agg(count("*").alias("count"))

    # Compute event counts for dead patients
    dead_count_events = dead_events.groupBy("patientid").agg(count("*").alias("count"))
    
    # Compute event count metrics for alive patients
    alive_metrics = alive_count_events.agg(avg("count").alias("avg"),
                             min("count").alias("min"),
                             max("count").alias("max"))
    alive_metrics = alive_metrics.select(
        col("avg").cast("float"),  # Keep "avg" as float
        
        col("min").cast("int"),
        col("max").cast("int")
    )
    #alive_metrics.printSchema()

    # Compute event count metrics for dead patients
    dead_metrics = dead_count_events.agg(avg("count").alias("avg"),
                             min("count").alias("min"),
                             max("count").alias("max")) 
    dead_metrics = dead_metrics.select(
        col("avg").cast("float"),  # Keep "avg" as float
         
        col("min").cast("int"),
        col("max").cast("int"))
    data = [(0.0, 0.0, 0.0)]
    columns = ["avg", "min", "max"]
    alive_statistics = alive_metrics
    dead_statistics = dead_metrics

    return alive_statistics, dead_statistics


def encounter_count_metrics(alive_events, dead_events):
    '''
    param: two spark DataFrame: alive_events, dead_events
    return: two spark DataFrame

    Task3: Compute average, median, min and max of encounter counts 
    for alive and dead patients respectively
    +------+--------+------+------+                                             
    |  avg | median | min  | max  |
    +------+--------+------+------+
    |value1| value2 |value3|value4|
    +------+--------+------+------+
    note: 
    1.keep alive dataframe and dead dateframe respectively, in this case, you will get 2 (1 for alive and 1 for dead) dataframe.
    2.please keep same column name as example showed before!
    3.average computed with mean(), DO NOT need to round the results.
    4.for the median section, when the array is even, do not use the average of the two middle elements, you can choose
      the smallest value around the midpoint of an even array. Or, 'percentile_approx' func may useful, the link:
      https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.functions.percentile_approx.html. 

    '''
    # Compute event counts for alive patients
    alive_count_events = alive_events.groupBy("patientid").agg(
    countDistinct("etimestamp").alias("unique_date_count")
)

    # Compute event counts for dead patients
    dead_count_events = dead_events.groupBy("patientid").agg(
    countDistinct("etimestamp").alias("unique_date_count")
)
    
    # Compute event count metrics for alive patients
    alive_metrics = alive_count_events.agg(
    mean("unique_date_count").alias("avg"),
    expr("percentile_approx(unique_date_count, array(0.5))").alias("median"),
    min("unique_date_count").alias("min"),
    max("unique_date_count").alias("max")
    )

    #alive_metrics.printSchema()
    
    # Compute event count metrics for dead patients
    dead_metrics = dead_count_events.agg(mean("unique_date_count").alias("avg"),expr("percentile_approx(unique_date_count, array(0.5))").alias("median"),
                             min("unique_date_count").alias("min"),
                             max("unique_date_count").alias("max")) 
    
    # Extract the first element from the list returned by percentile_approx
    alive_metrics = alive_metrics.withColumn("median", alive_metrics["median"].getItem(0))
    dead_metrics = dead_metrics.withColumn("median", dead_metrics["median"].getItem(0))


    #data = [(0.0, 0.0, 0.0, 0.0)]
    columns = ["avg", "median", "min", "max"]
    # Create DataFrames with the selected values
    alive_encounter_res = alive_metrics
    dead_encounter_res = dead_metrics
    # alive_encounter_res = spark.createDataFrame(data=data, schema=columns)
    # dead_encounter_res= spark.createDataFrame(data=data, schema=columns)

    return alive_encounter_res, dead_encounter_res


def record_length_metrics(alive_events, dead_events):
    '''
    param: two spark DataFrame:alive_events, dead_events
    return: two spark DataFrame

    Task4: Record length metrics
    Compute average, median, min and max of record lengths
    for alive and dead patients respectively
    +------+--------+------+------+                                             
    |  avg | median | min  | max  |
    +------+--------+------+------+
    |value1| value2 |value3|value4|
    +------+--------+------+------+
    note: 
    1.keep alive dataframe and dead dateframe respectively, in this case, you will get 2 (1 for alive and 1 for dead) dataframe.
    2.please keep same column name as example showed before!
    3.average computed with mean(), DO NOT round the results.

    '''
    # Convert etimestamp to a date type
    alive_events = alive_events.withColumn("etimestamp", col("etimestamp").cast("date"))

    # Group by patientid and calculate max, min, and date difference
    alive_record_lengths = alive_events.groupBy("patientid").agg(
        max("etimestamp").alias("max_date"),
        min("etimestamp").alias("min_date"),
        datediff(max("etimestamp"), min("etimestamp")).alias("date_difference")
    )

    # Compute metrics for alive patients
    alive_metrics = alive_record_lengths.agg(
        mean("date_difference").alias("avg"),
        expr("percentile_approx(date_difference, array(0.5))").alias("median"),
        min("date_difference").alias("min"),
        max("date_difference").alias("max")
    )
    # Extract the first element of the median array and cast to int
    alive_metrics = alive_metrics.withColumn("median", col("median")[0].cast("int"))
    alive_metrics = alive_metrics.withColumn("avg", col("avg").cast("float"))

    # Cast min and max to int
    alive_metrics = alive_metrics.withColumn("min", col("min").cast("int"))
    alive_metrics = alive_metrics.withColumn("max", col("max").cast("int"))



    # Show the result
    a = alive_metrics.dtypes

     #----
    dead_events_events = dead_events.withColumn("etimestamp", col("etimestamp").cast("date"))

    # Group by patientid and calculate max, min, and date difference
    dead_record_lengths = dead_events.groupBy("patientid").agg(
        max("etimestamp").alias("max_date"),
        min("etimestamp").alias("min_date"),
        datediff(max("etimestamp"), min("etimestamp")).alias("date_difference")
    )

    # Compute metrics for alive patients
    dead_metrics = dead_record_lengths.agg(
        mean("date_difference").alias("avg"),
        expr("percentile_approx(date_difference, array(0.5))").alias("median"),
        min("date_difference").alias("min"),
        max("date_difference").alias("max")
    )
    # Extract the first element of the median array and cast to int
    dead_metrics = dead_metrics.withColumn("median", col("median")[0].cast("int"))
    dead_metrics = dead_metrics.withColumn("avg", col("avg").cast("float"))

    # Cast min and max to int
    dead_metrics = dead_metrics.withColumn("min", col("min").cast("int"))
    dead_metrics = dead_metrics.withColumn("max", col("max").cast("int"))


    # Show the result
    b = dead_metrics.dtypes  
    data = [(0.0, 0.0, 0.0, 0.0)]
    columns = ["avg", "median", "min", "max"]
    alive_recordlength_res = alive_metrics
    dead_recordlength_res = dead_metrics

    return alive_recordlength_res, dead_recordlength_res

def Common(alive_events, dead_events):
    '''
    param: two spark DataFrame: alive_events, dead_events
    return: six spark DataFrame
    Task 5: Common diag/lab/med
    Compute the 5 most frequently occurring diag/lab/med
    for alive and dead patients respectively
    +------------+----------+                                                       
    |   eventid  |diag_count|
    +------------+----------+
    |  DIAG999999|      9999|
    |  DIAG999999|      9999|
    |  DIAG999999|      9999|
    |  DIAG999999|      9999|
    |  DIAG999999|      9999|
    +------------+----------+

    +------------+----------+                                                       
    |   eventid  | lab_count|
    +------------+----------+
    |  LAB999999 |      9999|
    |  LAB999999 |      9999|   
    |  LAB999999 |      9999|
    |  LAB999999 |      9999|
    +------------+----------+

    +------------+----------+                                                       
    |   eventid  | med_count|
    +------------+----------+
    |  DRUG999999|      9999|
    |  DRUG999999|      9999|
    |  DRUG999999|      9999|
    |  DRUG999999|      9999|
    |  DRUG999999|      9999|
    +------------+----------+
    note:
    1.keep alive dataframe and dead dateframe respectively, in this case, you will get 6 (3 for alive and 3 for dead) dataframe.
    2.please keep same column name as example showed before!
    '''
    # data = [("DIAG999999", 999),("DIAG999999", 999),("DIAG999999", 999),("DIAG999999", 999),("DIAG999999", 999)]
    # columns = ["eventid", "diag_count"]
    # alive_diag= spark.createDataFrame(data=data, schema=columns)
    # dead_diag= spark.createDataFrame(data=data, schema=columns)

    # data = [("LAB999999", 999),("LAB999999", 999),("LAB999999", 999),("LAB999999", 999),("LAB999999", 999)]
    # columns = ["eventid", "drug_count"]
    # alive_lab= spark.createDataFrame(data=data, schema=columns)
    # dead_lab= spark.createDataFrame(data=data, schema=columns)

    # data = [("DRUG999999", 999),("DRUG999999", 999),("DRUG999999", 999),("DRUG999999", 999),("DRUG999999", 999)]
    # columns = ["eventid", "med_count"]
    # alive_med= spark.createDataFrame(data=data, schema=columns)
    # dead_med= spark.createDataFrame(data=data, schema=columns)
    #-----------given above but not used since createDataFrame not working right now due to PyJ4 error possibly due to java or pyspark version issue om environemnt
    # Filter rows where value starts with "DIAG"
    alive_diag_counts = alive_events.filter(col("eventid").startswith("DIAG")).groupBy("eventid").agg(count("*").alias("diag_count"))

    # Filter rows where value starts with "DIAG"
    dead_diag_counts = dead_events.filter(col("eventid").startswith("DIAG")).groupBy("eventid").agg(count("*").alias("diag_count"))

    # Filter rows where eventid starts with "LAB"
    alive_lab_counts = alive_events.filter(col("eventid").startswith("LAB")).groupBy("eventid").agg(count("*").alias("lab_count"))
    dead_lab_counts = dead_events.filter(col("eventid").startswith("LAB")).groupBy("eventid").agg(count("*").alias("lab_count"))

    # Filter rows where eventid starts with "DRUG"
    alive_drug_counts = alive_events.filter(col("eventid").startswith("DRUG")).groupBy("eventid").agg(count("*").alias("drug_count"))
    dead_drug_counts = dead_events.filter(col("eventid").startswith("DRUG")).groupBy("eventid").agg(count("*").alias("drug_count"))
    # Show the result
    print("Alive Diag Counts:")
    alive_diag_counts.select("eventid", "diag_count")
    dead_diag_counts.select("eventid", "diag_count")

    #get top 5 most common occuring    
    # Order by count in descending order and select the top 5 rows
    alive_diag = alive_diag_counts.orderBy(col("diag_count").desc()).limit(5)
    dead_diag = dead_diag_counts.orderBy(col("diag_count").desc()).limit(5)   
     
    alive_lab = alive_lab_counts.orderBy(col("lab_count").desc()).limit(5)
    dead_lab = dead_lab_counts.orderBy(col("lab_count").desc()).limit(5)      
        
    alive_med = alive_drug_counts.orderBy(col("drug_count").desc()).limit(5)
    dead_med = dead_drug_counts.orderBy(col("drug_count").desc()).limit(5)      
    return alive_diag, alive_lab, alive_med, dead_diag, dead_lab, dead_med


def main():
    '''
    DO NOT MODIFY THIS FUNCTION.
    '''
    # You may change the following path variable in coding but switch it back when submission.

    path1 = './data/events.csv'
    schema1 = StructType([
        StructField("patientid", IntegerType(), True),
        StructField("eventid", StringType(), True),
        StructField("eventdesc", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("value", FloatType(), True)])

    path2 = './data/mortality.csv'
    schema2 = StructType([
        StructField("patientid", IntegerType(), True),
        StructField("timestamp", StringType(), True),
        StructField("label", IntegerType(), True)])

    events = read_csv(spark, path1, schema1)
    events = events.select(events.patientid, events.eventid, to_date(events.timestamp).alias("etimestamp"), events.value)

    mortality = read_csv(spark, path2, schema2)
    mortality = mortality.select(mortality.patientid, to_date(mortality.timestamp).alias("mtimestamp"), mortality.label)

    alive_events, dead_events = split_alive_dead(events, mortality)

    #Compute the event count metrics
    start_time = time.time()
    alive_statistics, dead_statistics = event_count_metrics(alive_events, dead_events)
    end_time = time.time()
    print(("Time to compute event count metrics: " + str(end_time - start_time) + "s"))
    alive_statistics.show()
    dead_statistics.show()

    #Compute the encounter count metrics
    start_time = time.time()
    alive_encounter_res, dead_encounter_res = encounter_count_metrics(alive_events, dead_events)
    end_time = time.time()
    print(("Time to compute encounter count metrics: " + str(end_time - start_time) + "s"))
    #print(encounter_count)
    alive_encounter_res.show()
    dead_encounter_res.show()


    #Compute record length metrics
    start_time = time.time()
    alive_recordlength_res, dead_recordlength_res = record_length_metrics(alive_events, dead_events)
    end_time = time.time()
    print(("Time to compute record length metrics: " + str(end_time - start_time) + "s"))
    alive_recordlength_res.show()
    dead_recordlength_res.show()


    #Compute Common metrics
    start_time = time.time()
    alive_diag, alive_lab, alive_med, dead_diag, dead_lab, dead_med = Common(alive_events, dead_events)
    end_time = time.time()
    print(("Time to compute event count metrics: " + str(end_time - start_time) + "s"))
    alive_diag.show()
    alive_lab.show()
    alive_med.show()
    dead_diag.show()
    dead_lab.show()
    dead_med.show()

    

if __name__ == "__main__":
    main()

