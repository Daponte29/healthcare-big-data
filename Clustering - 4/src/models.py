# Models extracted from BD4H_HW3.ipynb

class Medication:
    "medication class"
    def __init__(self, patientID, date, medicine):
        self.patientID = patientID
        self.date = date
        self.medicine = medicine

class LabResult:
    "lab class"
    def __init__(self, patientID, date, resultName, value):
        self.patientID = patientID
        self.date = date
        self.resultName = resultName
        self.value = value

class Diagnostic:
    "diagnostic class"
    def __init__(self, patientID, code, date):
        self.patientID = patientID
        self.date = date
        self.code = code
