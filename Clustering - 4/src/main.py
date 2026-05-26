import argparse
from pyspark.sql import SparkSession
from data_loader import load_rdd_raw_data, loadLocalRawData
from phenotyping import transform
from features import constructDiagnosticFeatureTuple, constructMedicationFeatureTuple, constructLabFeatureTuple, construct
from clustering import clustering

def main():
    parser = argparse.ArgumentParser(description="Healthcare Clustering Job")
    parser.add_argument("--data-uri", type=str, required=True, help="S3 URI or local path to the input data directory")
    parser.add_argument("--output-uri", type=str, required=True, help="S3 URI or local path for the outputs")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("HealthcareClusteringEMR").getOrCreate()
    sc = spark.sparkContext
    logger = spark._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)

    print(f"Loading data from {args.data_uri}...")
    medication, lab_result, diagnostic = load_rdd_raw_data(spark, args.data_uri)
    candidate_medication, candidate_lab, candidate_diagnostic = loadLocalRawData(spark, args.data_uri)

    print("Generating phenotype labels...")
    phenotype_label = transform(medication, lab_result, diagnostic)

    print("Constructing sparse features...")
    # Filtered Feature extraction
    filteredFeatureTuples = constructDiagnosticFeatureTuple(diagnostic, candidate_diagnostic).union(
        constructLabFeatureTuple(lab_result, candidate_lab)
    ).union(
        constructMedicationFeatureTuple(medication, candidate_medication)
    )

    rawFeatures = construct(filteredFeatureTuples)

    print("Running KMeans and GMM Clustering...")
    kMeansPurity, gmmPurity = clustering(phenotype_label, rawFeatures, k=3)

    print(f"K-Means Purity: {kMeansPurity}")
    print(f"GMM Purity: {gmmPurity}")

    # Output simple results file to S3
    results = [
        f"K-Means Purity: {kMeansPurity}",
        f"GMM Purity: {gmmPurity}"
    ]
    rdd_out = sc.parallelize(results)
    
    output_path = f"{args.output_uri.rstrip('/')}/purity_metrics.txt"
    print(f"Saving output to {output_path}")
    rdd_out.coalesce(1).saveAsTextFile(output_path)

    spark.stop()

if __name__ == "__main__":
    main()
