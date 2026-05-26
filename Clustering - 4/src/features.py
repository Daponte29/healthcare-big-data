from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark import RDD
from typing import Tuple, Set

FeatureTuple = Tuple[Tuple[str, str], float]

def constructDiagnosticFeatureTuple(diagnostic: RDD, candidateCode: Set = None) -> RDD:
    """
    Aggregate feature tuples from diagnostic with COUNT aggregation
    """
    if candidateCode is not None:
        diagnostic = diagnostic.filter(lambda x: x.code in candidateCode)

    diag_feature = diagnostic.map(lambda x: ((x.patientID, x.code), 1)) \
                             .reduceByKey(lambda x, y: x + y) \
                             .map(lambda x: ((x[0][0], x[0][1]), x[1]))
    return diag_feature

def constructMedicationFeatureTuple(medication: RDD, candidateMedication: Set = None) -> RDD:
    """
    Aggregate feature tuples from medication with COUNT aggregation 
    """
    if candidateMedication is not None:
        medication = medication.filter(lambda x: x.medicine in candidateMedication)

    medications_feature = medication.map(lambda x: ((x.patientID, x.medicine), 1)) \
                                    .reduceByKey(lambda x, y: x + y) \
                                    .map(lambda x: ((x[0][0], x[0][1]), x[1]))
    return medications_feature

def constructLabFeatureTuple(labResult: RDD, candidateLab: Set = None) -> RDD:
    """
    Aggregate feature tuples from lab result, using AVERAGE aggregation
    """
    if candidateLab is not None:
        labResult = labResult.filter(lambda x: x.resultName in candidateLab)

    lab_sum_count = labResult.map(lambda x: ((x.patientID, x.resultName), (x.value, 1))) \
                             .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

    lab_feature = lab_sum_count.map(lambda x: ((x[0][0], x[0][1]), x[1][0] / x[1][1]))
    return lab_feature

def construct(feature: RDD) -> RDD:
    """
    Given a feature tuples RDD, construct features in vector format for each patient. 
    Feature name should be mapped to some index and convert to sparse feature format.
    """
    sc = SparkSession.builder.getOrCreate().sparkContext

    # Collect unique feature names and assign indices
    feature_names = feature.map(lambda x: x[0][1]).distinct().collect()
    feature_index = dict(zip(sorted(feature_names), range(len(feature_names))))

    # Broadcast feature index to all nodes
    sc_feature_map = sc.broadcast(feature_index)

    # Transform input feature tuples
    patient_and_features = feature.map(lambda x: (x[0][0], (sc_feature_map.value[x[0][1]], x[1]))).groupByKey()

    # Create sparse vectors for each patient
    result = patient_and_features.map(lambda x: (
        x[0],
        Vectors.sparse(len(sc_feature_map.value), [(feature_idx, value) for feature_idx, value in sorted(x[1])])
    ))

    return result
