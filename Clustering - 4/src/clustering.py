from pyspark.sql import DataFrame
from pyspark.ml.feature import StandardScaler, PCA
from pyspark.ml.clustering import KMeans, GaussianMixture

def getPurity(cluster_assignment_and_label):
    """
    Calculate the purity of clustering
    Param: cluster_assignment_and_label: RDD in the tuple format ((assigned_cluster_id, class), number)
    Return: purity
    """
    clusters_labels = cluster_assignment_and_label.reduceByKey(lambda x, y: x + y)

    # Group by cluster assignment and class, then find the maximum count for each cluster
    max_counts = clusters_labels.groupBy(lambda x: x[0][0]).flatMap(lambda x: [max(x[1], key=lambda y: y[1])])

    # Sum up the maximum counts to get the total correct assignments
    total_correct = max_counts.map(lambda x: x[1]).sum()

    # Sum up the total occurrences for normalization
    total_occurrences = clusters_labels.map(lambda x: x[1]).sum()

    # Calculate purity
    purity = total_correct / total_occurrences

    return purity

def clustering(phenotypeLabel, rawFeatures, k=3):
    """
    Apply KMeans and GMM clustering
    """
    print('phenotypeLabel: ', phenotypeLabel.count())
    standardizer = StandardScaler(withMean=True, withStd=True)
    df_features = rawFeatures.toDF(["id", "features"])
    scaler_model = standardizer.setInputCol("features").setOutputCol("scaled_features").fit(df_features)
    df_features = scaler_model.transform(df_features)

    # Reduce dimension
    pca = PCA(k=10, inputCol="scaled_features", outputCol="pca_features")
    pca_model = pca.fit(df_features)
    df_features = pca_model.transform(df_features)

    if not isinstance(phenotypeLabel, DataFrame):
        phenotypeLabel = phenotypeLabel.toDF(["id", "label"])

    # 1. K Means Clustering using pyspark ml
    kmeans = KMeans(k=k, seed=6250, featuresCol="pca_features")
    kmeans_model = kmeans.fit(df_features)
    kmeans_predictions = kmeans_model.transform(df_features)

    # 2. GMM Clustering using spark ml
    gmm = GaussianMixture().setK(k).setSeed(6250).setFeaturesCol("pca_features").setPredictionCol("prediction")
    gmm_model = gmm.fit(df_features)
    gmm_predictions = gmm_model.transform(df_features)

    kmeans_joined = kmeans_predictions.join(phenotypeLabel, "id").rdd.map(lambda row: ((row.prediction, row.label), 1)).reduceByKey(lambda a, b: a + b)
    gmm_joined = gmm_predictions.join(phenotypeLabel, "id").rdd.map(lambda row: ((row.prediction, row.label), 1)).reduceByKey(lambda a, b: a + b)

    kmeans_purity = getPurity(kmeans_joined)
    gmm_purity = getPurity(gmm_joined)
    
    return kmeans_purity, gmm_purity
