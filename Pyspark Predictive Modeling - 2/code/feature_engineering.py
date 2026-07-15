from pyspark.sql.functions import when, sort_array, datediff, to_date, max as max_, lit, col, collect_list, row_number, concat_ws, format_number, concat, monotonically_increasing_id, expr
import random
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType
import shutil
import os
from pyspark.sql.window import Window
import operator
import pyspark.sql.functions as F
#---
from pyspark.sql.functions import col, date_sub
from pyspark.sql.functions import max as pyspark_max
from pyspark.sql.functions import date_sub, max
from pyspark.sql import functions as F


spark = SparkSession.builder.appName('Read CSV File into DataFrame').getOrCreate()
sc = spark.sparkContext

def read_csv(spark, file, schema):
    return spark.read.csv(file, header=False, schema=schema)

def calculate_index_dates(events, mortality):
    '''
    INPUT1:
    events_df read from events.csv
    e.g.
    +---------+------------+----------+-----+
    |patientid|     eventid|etimestamp|value|
    +---------+------------+----------+-----+
    |    20459|DIAG42872402|1994-12-04|  1.0|
    
    INPUT2:
    mortality_df read from mortality.csv
    +---------+----------+-----+
    |patientid|mtimestamp|label|
    +---------+----------+-----+
    |    13905|2000-01-30|    1|

    OUTPUT:
    index_df
    index_date is datetime.date format
    e.g.
    +---------+----------+
    |patientid|index_date|
    +---------+----------+
    |    20459|2000-09-19|
    |    13905|1999-12-31|
    +---------+----------+
    '''
    
    
    # # The output coloumns should have the name (patientid, index_date)
    # index_dates = [(20459, '2000-09-19'),
    #     (5206, '2000-08-04')]
    # columns = ["patientid", "index_date"]
    # df = spark.createDataFrame(data=index_dates, schema=columns)
    #---------given above
    
    
    deceased_patient_ids = mortality.select("patientid").distinct()
    
    # Create alive and deceased Spark DataFrames
    alive_events = events.join(deceased_patient_ids, "patientid", "left_anti")
    
       # Calculate the index_date 30 days prior to mtimestamp
    dead_index_date = mortality.withColumn(
        "index_date",
        date_sub(col("mtimestamp"), 30).cast("date")
    ).select("patientid", "index_date")
    
       # Get the maximum etimestamp for each patientid
    alive_index_date = alive_events.groupBy("patientid").agg(
        max_("etimestamp").cast("date").alias("index_date")
    )
    
 
    # Concatenate both alive and dead
    df = alive_index_date.union(dead_index_date)

    return df

def filter_events(events, index_dates):
    # TODO: filtered events should have the same input column of original events, select the corresponding columns and revise test as well
    '''
    INPUT:
    events: created events df, e.g.
    +---------+------------+----------+-----+
    |patientid|     eventid|etimestamp|value|
    +---------+------------+----------+-----+
    |    20459|DIAG42872402|1994-12-04|  1.0|
    +---------+------------+----------+-----+
    
    index_dates: created index_date df, e.g
    +---------+----------+
    |patientid|index_date|
    +---------+----------+
    |    20459|2000-09-19|
    +---------+----------+

    OUTPUT:
    filtered: e.g.
    +---------+--------------+----------+-----+
    |patientid|   eventid    |etimestamp|value|
    +---------+--------------+----------+-----+
    |    20459|'DIAG42872404'|1999-12-04|  1.0|
    |    19992|'DIAG42872403'|1995-12-04|  1.0|
    +---------+--------------+----------+-----+
    '''
    # Remove the events that are not in the observation window


    # filtered = [(20459, 'DIAG42872404', '1999-12-04', 1.0)]
    # columns = ["patientid", "eventid", "etimestamp", "value"]
    # df = spark.createDataFrame(data=filtered, schema=columns)
    # above given---------------------
    
    # Rename columns in the 'df' DataFrame to avoid ambiguity
    index_dates = index_dates.withColumnRenamed("patientid", "df_patientid").withColumnRenamed("index_date", "df_index_date")
    # Define the observation window conditions
    observation_window_conditions = (
        (col("etimestamp") >= (col("df_index_date") - expr("INTERVAL 2000 DAYS"))) &
        (col("etimestamp") <= col("df_index_date"))
    )

    # Apply the filtering conditions to each patient group
    df = (
        events
        .join(index_dates, events.patientid == index_dates.df_patientid, "inner")
        .filter(observation_window_conditions)
        .drop("df_index_date", "df_patientid")  # Drop the last two columns
    )

    
    return df

def aggregate_events(filtered):
    '''
    INPUT:
    filtered
    e.g.
    +---------+----------+----------+-----+
    |patientid|   eventid|etimestamp|value|
    +---------+----------+----------+-----+
    |    20459|LAB3013603|2000-09-19|  0.6|

    OUTPUT:
    patient_features
    e.g.
    +---------+------------+-------------+
    |patientid|     eventid|feature_value|
    +---------+------------+-------------+
    |     5206|DRUG19065818|            1|
    |     5206|  LAB3021119|            1|
    |    20459|  LAB3013682|           11|    
    +---------+------------+-------------+
    '''
    # Output columns should be (patientid, eventid, feature_value)


    # features = [(20459, 'LAB3013682', 11)]
    # columns = ["patientid", "eventid", "feature_value"]
    # df = spark.createDataFrame(data=features, schema=columns)
    # #--above given
    
    # Group by patientid and eventid, and count occurrences
    feature_df = (
        filtered
        .groupBy("patientid", "eventid")
        .agg(F.count("*").alias("feature_value"))
    )
    return feature_df

def generate_feature_mapping(agg_events):
    '''
    INPUT:
    agg_events
    e.g.
    +---------+------------+-------------+
    |patientid|     eventid|feature_value|
    +---------+------------+-------------+
    |     5206|DRUG19065818|            1|
    |     5206|  LAB3021119|            1|
    |    20459|  LAB3013682|           11|
    +---------+------------+-------------+

    OUTPUT:
    event_map
    e.g.
    +----------+-----------+
    |   eventid|event_index|
    +----------+-----------+
    |DIAG132797|          0|
    |DIAG135214|          1|
    |DIAG137829|          2|
    |DIAG141499|          3|
    |DIAG192767|          4|
    |DIAG193598|          5|
    +----------+-----------+
    '''
    # Hint: pyspark.sql.functions: monotonically_increasing_id
    # Output colomns should be (eventid, event_index)
    

    # event_map = [("DIAG132797", 0)]
    # columns = ["eventid", "event_index"]
    # df = spark.createDataFrame(data=event_map, schema=columns)
    #given above
    # Sort the DataFrame by 'eventid' in ascending order
    sorted_feature_df = (
        agg_events
        .select("eventid")
        .distinct()  # Get unique eventid
        .orderBy("eventid")  # Sort by eventid in ascending order
    )

    # Show the sorted DataFrame
    #sorted_feature_df.show()
    # Define a window specification to order by 'eventid'
    window_spec = Window.orderBy("eventid")

    # Add a new column 'event_index' using dense_rank window function
    feature_map_df = sorted_feature_df.withColumn("event_index", F.dense_rank().over(window_spec) - 1)
    
    
    
    
    return feature_map_df

def normalization(agg_events):
    '''
    INPUT:
    agg_events
    e.g.
    +---------+------------+-------------+
    |patientid|     eventid|feature_value|
    +---------+------------+-------------+
    |     5206|DRUG19065818|            1|
    |     5206|  LAB3021119|            1|
    |    20459|  LAB3013682|           11|   


    OUTPUT:
    normalized
    e.g.
    +---------+------------+------------------------+
    |patientid|     eventid|normalized_feature_value|
    +---------+------------+------------------------+
    |     5206|DRUG19065818|                   1.000|
    |     5206|  LAB3021119|                   1.000|
    |    20459|  LAB3013682|                   0.379|   
    +---------+------------+------------------------+
    '''
    # Output columns should be (patientid, eventid, normalized_feature_value)
    # Note: round the normalized_feature_value to 3 places after decimal: use round() in pyspark.sql.functions

    # event_map = [("5206", "DRUG19065818", 1.000)]
    # columns = ["patientid", "eventid", "normalized_feature_value"]
    # df = spark.createDataFrame(data=event_map, schema=columns)
    #Given above not used due to Java version not allowing createDatFrame to wwork
    
    
    # Assuming 'feature_df' is your PySpark DataFrame
    window_spec = Window.partitionBy("eventid")

    # Calculate the maximum feature_value for each eventid
    max_feature_value = F.max("feature_value").over(window_spec)

    # Normalize the feature_value column by dividing each value by the max_feature_value
    normalized_feature_df = (
        agg_events
        .withColumn("max_feature_value", max_feature_value)
        .withColumn(
            "normalized_feature_value",
            F.when(col("max_feature_value") != 0, col("feature_value") / col("max_feature_value")).otherwise(col("feature_value"))
        )
        .drop("max_feature_value")
        .drop("feature_value")
        .withColumn("normalized_feature_value", F.round("normalized_feature_value", 3))
    )
    
    return normalized_feature_df

def svmlight_convert(normalized, event_map):
    '''
    INPUT:
    normalized
    e.g.
    +---------+------------+------------------------+
    |patientid|     eventid|normalized_feature_value|
    +---------+------------+------------------------+
    |    20459|  LAB3023103|                   0.062|
    |    20459|  LAB3027114|                   1.000|
    |    20459|  LAB3007461|                   0.115|
    +---------+------------+------------------------+

    event_map
    e.g.
    +----------+-----------+
    |   eventid|event_index|
    +----------+-----------+
    |DIAG132797|          0|
    |DIAG135214|          1|
    |DIAG137829|          2|
    +----------+-----------+

    OUTPUT:    
    svmlight: patientid, sparse_feature
    sparse_feature is a list containing: feature pairs
    earch feature pair is a string: "event_index:normalized_feature_val"
    e.g
    +---------+-------------------+
    |patientid|   sparse_feature  |
    +---------+-------------------+
    |    19992|[2:1.000, 9:1.000] |
    |    19993|[2:0.667, 12:0.500]|
    +---------+-------------------+
    '''
    # Output columns should be (patientid, sparse_feature)
    # Note: for normalized_feature_val, when convert it to string, save 3 digits after decimal including "0": use format_number() in pyspark.sql.functions
    # Hint:
    #         pyspark.sql.functions: concat_with(), collect_list()
    #         pyspark.sql.window: Window.partitionBy(), Window.orderBy()

    # svmlight = [("19992", ["2:1.000", "9:1.000"])]
    # columns = ["patientid", "sparse_feature"]
    # df = spark.createDataFrame(data=svmlight, schema=columns)
    # #Given above, not using due to .createDataFrame not working
    
    # First create dataframe with normalized_feature_df replacing eventid with featuremap numbers
    #Replace eventid with event_index first
    df_1 = normalized.join(event_map, on ="eventid", how="inner")

    df_1 = df_1.select("patientid", "event_index", "normalized_feature_value")

    #df_1.show()

    #delete all rows where normalized_feature_value is 0 since we are skipping these it says in HW
    df_1 = df_1.filter(col("normalized_feature_value") != 0)
    
    grouped_df = (
        df_1
        .groupBy("patientid")
        .agg(
            F.collect_list(
                F.struct("event_index", "normalized_feature_value")
            ).alias("event_list")
        )
        # Order the DataFrame by event_index within each group
        .orderBy("patientid", F.asc("event_list.event_index"))
    )

    # Select relevant columns and show the result
    sorted_df_1 = grouped_df.select("patientid", "event_list.event_index", "event_list.normalized_feature_value")

    # Group by patientid, collect a list of structs containing eventid and normalized_feature_value
    # Group by patientid, collect a list of (event_index, normalized_feature_value) structs
    svmlight_df = (
        sorted_df_1
        .groupBy("patientid")
        .agg(
            F.sort_array(
                F.flatten(
                    F.collect_list(
                        F.expr("arrays_zip(event_index, normalized_feature_value)")
                    )
                )
            ).alias("zipped_values")
        )
        .withColumn("sparse_feature", F.expr("""
            transform(zipped_values, x -> 
                concat_ws(':', x.event_index, format_number(x.normalized_feature_value, '0.000'))
            )
        """))
        .select("patientid", "sparse_feature")
        .orderBy("patientid")
    )

    
    return svmlight_df 

def svmlight_samples(svmlight, mortality):
    '''
    INPUT:
    svmlight
    +---------+--------------------+
    |patientid|      sparse_feature|
    +---------+--------------------+
    |     5206|[4:1.000, 5:1.000...|
    |    13905|[1:1.000, 11:1.00...|
    |    18676|[0:1.000, 2:1.000...|
    |    20301|[10:1.000, 12:1.0...|
    |    20459|[136:0.250, 137:1...|
    +---------+--------------------+

    mortality
    +---------+----------+-----+
    |patientid|mtimestamp|label|
    +---------+----------+-----+
    |    13905|2000-01-30|    1|
    |    18676|2000-02-03|    1|
    |    20301|2002-08-08|    1|
    +---------+----------+-----+

    OUTPUT
    samples
    +---------+--------------------+-------------+--------------------+
    |patientid|      sparse_feature|other columns|        save_feature|
    +---------+--------------------+-------------+--------------------+
    |     5206|[4:1.000, 5:1.000...|     ...     |0 4:1.000 5:1.000...|
    |    13905|[1:1.000, 11:1.00...|     ...     |1 1:1.000 11:1.00...|
    |    18676|[0:1.000, 2:1.000...|     ...     |1 0:1.000 2:1.000...|
    |    20301|[10:1.000, 12:1.0...|     ...     |1 10:1.000 12:1.0...|
    |    20459|[136:0.250, 137:1...|     ...     |0 136:0.250 137:1...|
    +---------+--------------------+-------------+--------------------+
    '''

    # Task: create a new DataFrame by adding a new colum in "svmlight".
    # New column name is "save_feature" which is a String including target 
    # and sparse feature in SVMLight format;
    # New DataFrame name is "samples"
    # You can have other columns in "samples"    
    # Hint:
    #         pyspark.sql.functions: concat_with
    
    # samples = [("5206", "0 4:1.000 5:1.000")]
    # columns = ["patientid", "save_feature"]
    # df = spark.createDataFrame(data=samples, schema=columns)
    #given above but createdataframe method wont work due to java package
    joined_df = svmlight.join(mortality, on="patientid", how="left_outer")
    #joined_df.show()
    
    # Use PySpark functions to conditionally update the sparse_feature column
    updated_df = joined_df.withColumn(
        "save_feature",
        when(col("label") == 1, concat_ws(" ", lit("1"), col("sparse_feature")))
        .otherwise(concat_ws(" ", lit("0"), col("sparse_feature")))
    )
    svmlight_samples_df = updated_df.select("patientid", "save_feature").orderBy("patientid")
    svmlight_samples_df = svmlight.join(svmlight_samples_df, on="patientid", how="left_outer")
    return svmlight_samples_df

def train_test_split(samples, train_path, test_path):
    
    # DO NOT change content below
    samples = samples.randomSplit([0.2, 0.8], seed=48)

    testing = samples[0].select(samples[0].save_feature)
    training = samples[1].select(samples[1].save_feature)

    #save training and tesing data
    if os.path.exists(train_path):
        shutil.rmtree(train_path)

    training.write.option("escape","").option("quotes", "").option("delimiter"," ").text(train_path)

    if os.path.exists(test_path):
        shutil.rmtree(test_path)

    testing.write.option("escape","").option("quotes", "").option("delimiter"," ").text(test_path)
    


def main():
    '''
    CHANGE THE path1 AND path2 WHEN YOU NEEDED, AND SWITCH IT BACK WHEN SUBMITTED
    '''

    path1 = './data/raw/events.csv'
    #path1 = r"C:\Users\nolot\OneDrive\Desktop\Big Data Healthcare\HW2\hw2-2\hw2\data\events.csv"   # this path is used to test your model's performance while you run Part2.3, only use this path in Part2.3        
    
    schema1 = StructType([
        StructField("patientid", IntegerType(), True),
        StructField("eventid", StringType(), True),
        StructField("eventdesc", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("value", FloatType(), True)])

    path2 = './data/raw/mortality.csv'
    #path2 = r"C:\Users\nolot\OneDrive\Desktop\Big Data Healthcare\HW2\hw2-2\hw2\data\mortality.csv" 
    
    schema2 = StructType([
        StructField("patientid", IntegerType(), True),
        StructField("timestamp", StringType(), True),
        StructField("label", IntegerType(), True)])

    events = read_csv(spark, path1, schema1)
    events = events.select(events.patientid, events.eventid, to_date(events.timestamp).alias("etimestamp"), events.value)


    mortality = read_csv(spark, path2, schema2)
    mortality = mortality.select(mortality.patientid, to_date(mortality.timestamp).alias("mtimestamp"), mortality.label)

    index_dates = calculate_index_dates(events, mortality)
    print('index_dates')
    index_dates.show()

    filtered = filter_events(events, index_dates)
    print('filtered')
    filtered.show()

    agg_events = aggregate_events(filtered)
    print('agg_events')
    agg_events.show()

    event_map = generate_feature_mapping(agg_events)
    print('event_map')
    event_map.show()

    normalized = normalization(agg_events)
    print('normalized')
    normalized.show()

    svmlight = svmlight_convert(normalized, event_map)
    print('svmlight')
    svmlight.show()

    samples = svmlight_samples(svmlight, mortality)
    print('svmlight samples')
    samples.show()

    train_test_split(samples, './output/training', './output/testing')


if __name__ == "__main__":
    main()
