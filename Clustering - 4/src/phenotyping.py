T1DM_DX = {"250.01", "250.03", "250.11", "250.13", "250.21", "250.23", "250.31", "250.33", "250.41", "250.43",
           "250.51", "250.53", "250.61", "250.63", "250.71", "250.73", "250.81", "250.83", "250.91", "250.93"}

T2DM_DX = {"250.3", "250.32", "250.2", "250.22", "250.9", "250.92", "250.8", "250.82", "250.7", "250.72", "250.6",
           "250.62", "250.5", "250.52", "250.4", "250.42", "250.00", "250.02"}

T1DM_MED = {"lantus", "insulin glargine", "insulin aspart", "insulin detemir", "insulin lente", "insulin nph", "insulin reg", "insulin,ultralente"}

T2DM_MED = {"chlorpropamide", "diabinese", "diabanase", "diabinase", "glipizide", "glucotrol", "glucotrol xl",
            "glucatrol ", "glyburide", "micronase", "glynase", "diabetamide", "diabeta", "glimepiride", "amaryl",
            "repaglinide", "prandin", "nateglinide", "metformin", "rosiglitazone", "pioglitazone", "acarbose",
            "miglitol", "sitagliptin", "exenatide", "tolazamide", "acetohexamide", "troglitazone", "tolbutamide",
            "avandia", "actos", "glipizide"}

DM_RELATED_DX = {"790.21", "790.22", "790.2", "790.29", "648.81", "648.82", "648.83", "648.84", "648", 
                 "648.01", "648.02", "648.03", "648.04", "791.5", "277.7", "V77.1", "256.4"}

abnormal_lab_values = {
    "HbA1c": 6.0,
    "Hemoglobin A1c": 6.0,
    "Fasting Glucose": 110.0,
    "Fasting blood glucose": 110.0,
    "fasting plasma glucose": 110.0,
    "Glucose": 110.0,
    "glucose": 110.0,
    "Glucose, Serum": 110.0
}

def transform(medication, labResult, diagnostic):
    """
    Transform given data set to a RDD of patients and corresponding phenotype
    param:
            medication: An RDD containing patient medication data.
            labResult: An RDD containing patient lab results.
            diagnostic: An RDD containing patient diagnostic data.
    return:
            phenotypeLabel: An RDD containing tuples. Each tuple contains a patient ID and a corresponding phenotype label.
                            the class label value should be 1, 2 and 3. 1 is Case, 2 is Control, 3 is Unknown.
    """
    no_t1dm_dx = diagnostic.filter(lambda x: x[2] not in T1DM_DX)
    case_patients_pre = no_t1dm_dx.filter(lambda x: x[2] in T2DM_DX).map(lambda x: (x[0], x[1])).reduceByKey(lambda x, y: min(x, y))

    t1dm_md = medication.filter(lambda x: x[2].lower() in T1DM_MED) \
                            .map(lambda x: (x[0], x[1])) \
                            .reduceByKey(lambda x, y: min(x, y))

    t2dm_md = medication.filter(lambda x: x[2].lower() in T2DM_MED) \
                            .map(lambda x: (x[0], x[1])) \
                            .reduceByKey(lambda x, y: min(x, y))

    early_t2dm_md = t1dm_md.join(t2dm_md).filter(lambda x: x[1][1] < x[1][0])

    case_patient_1 = case_patients_pre.subtractByKey(t1dm_md).map(lambda x: (x[0],1)).distinct()
    case_patient_2 = case_patients_pre.join(t1dm_md).subtractByKey(t2dm_md).map(lambda x: (x[0], 1)).distinct()
    case_patient_3 = case_patients_pre.join(t1dm_md).join(t2dm_md).join(early_t2dm_md).map(lambda x: (x[0], 1)).distinct()
    case_patients = case_patient_1.union(case_patient_2).union(case_patient_3).distinct()

    glucose_measure = labResult.filter(lambda x: "glucose" in x[2].lower())
    abnormal_lab = labResult.filter(lambda x: x[2] in abnormal_lab_values.keys() and float(x[3]) >= abnormal_lab_values[x[2]])
    diabetes_related_dx = diagnostic.filter(lambda x: x[2] in DM_RELATED_DX or x[2].startswith("250"))

    control_patients = glucose_measure \
                        .subtractByKey(abnormal_lab) \
                        .subtractByKey(diabetes_related_dx) \
                        .map(lambda x: (x[0], 2)).distinct()

    total_patients = diagnostic.map(lambda x: x[0]).union(labResult.map(lambda x: x[0])).union(medication.map(lambda x: x[0])).distinct()
    other_patients = total_patients.subtract(case_patients.map(lambda x: x[0])) \
                                   .subtract(control_patients.map(lambda x: x[0])) \
                                   .map(lambda x: (x, 3)).distinct()

    phenotypeLabel = case_patients.union(control_patients).union(other_patients)

    return phenotypeLabel
