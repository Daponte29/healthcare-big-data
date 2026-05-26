from datetime import datetime
from typing import Tuple, Set

def sql_date_parser(input, pattern="yyyy-MM-dd'T'HH:mm:ssX"):
    date_format = datetime.strptime(input, pattern)
    return date_format.date()

def load_rdd_raw_data(spark, data_dir: str):
    '''
    TODO: load the input .csv files in the datafolder as structured RDDs
    param:
            spark : the SparkSession
            data_dir : directory containing the CSV files
    return:
            rdd format for the medication, lab result and diagnostic
    '''
    lab_query = """
        SELECT Member_ID as patientID, Date_Resulted as date, Result_Name as resultName,
        CAST(REPLACE(Numeric_Result, ',', '') AS FLOAT) as value
        FROM lab_results_INPUT
        WHERE Numeric_Result IS NOT NULL AND Numeric_Result != ''
    """

    diagnostic_query = """
        SELECT e.Member_ID as patientID, e.Encounter_DateTime as date, dx.Code_ID as code
        FROM encounter_dx_INPUT dx
        JOIN encounter_INPUT e
        ON dx.Encounter_ID = e.Encounter_ID
    """

    medication_query = """
        SELECT Member_ID as patientID, Order_Date as date, Drug_Name as medicine
        FROM medication_orders_INPUT
    """

    file_name = [
        "encounter_INPUT.csv",
        "encounter_dx_INPUT.csv",
        "lab_results_INPUT.csv",
        "medication_orders_INPUT.csv"
    ]

    import os
    csv_files = [f"{data_dir.rstrip('/')}/raw/{x}" for x in file_name]

    for file in csv_files:
        df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(file)
        table_name = file.split("/")[-1].split(".")[0]
        df.createOrReplaceTempView(table_name)
        
    # Converting DataFrame to RDDs
    medication_rdd = spark.sql(medication_query).rdd
    lab_result_rdd = spark.sql(lab_query).rdd
    diagnostic_rdd = spark.sql(diagnostic_query).rdd

    return medication_rdd, lab_result_rdd, diagnostic_rdd

def loadLocalRawData(spark, data_dir: str) -> Tuple[Set[str], Set[str], Set[str]]:
    """
    Load filter files using Spark Context to support S3 URIs or Local Paths.
    """
    def _read_file(filename):
        path = f"{data_dir.rstrip('/')}/external/{filename}"
        return set([x.lower() for x in spark.sparkContext.textFile(path).collect()])

    candidateMedication = _read_file("med_filter.txt")
    candidateLab = _read_file("lab_filter.txt")
    candidateDiagnostic = _read_file("icd9_filter.txt")

    return (candidateMedication, candidateLab, candidateDiagnostic)

