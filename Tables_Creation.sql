USE Healthcare_Group_Project;
GO

-- Patient Table
CREATE TABLE Patient (
    PatientID UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
    Name VARCHAR(255) NOT NULL,
    Age INT CHECK (Age >= 0 AND Age <= 150),
    Gender VARCHAR(10),
    BloodType VARCHAR(5)
);

-- Admission Table
CREATE TABLE Admission (
    AdmissionID UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
    PatientID UNIQUEIDENTIFIER NOT NULL,
    DoctorID UNIQUEIDENTIFIER NOT NULL,
    HospitalID UNIQUEIDENTIFIER NOT NULL,
    InsuranceID UNIQUEIDENTIFIER NOT NULL,
    MedicationID UNIQUEIDENTIFIER NOT NULL,
    DateOfAdmission DATE,
    RoomNumber INT,
    AdmissionType VARCHAR(20),
    DischargeDate DATE,
    BillingAmount FLOAT CHECK (BillingAmount >= 0),
    MedicalCondition VARCHAR(50),
    TestResults VARCHAR(20),
    FOREIGN KEY (PatientID) REFERENCES Patient(PatientID),
    FOREIGN KEY (DoctorID) REFERENCES Doctor(DoctorID),
    FOREIGN KEY (HospitalID) REFERENCES Hospital(HospitalID),
    FOREIGN KEY (InsuranceID) REFERENCES InsuranceProvider(InsuranceID),
    FOREIGN KEY (MedicationID) REFERENCES Medication(MedicationID)
);

-- Doctor Table
CREATE TABLE Doctor (
    DoctorID UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
    Name VARCHAR(255) NOT NULL
);

-- Hospital Table
CREATE TABLE Hospital (
    HospitalID UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
    Name VARCHAR(255) NOT NULL
);

-- InsuranceProvider Table
CREATE TABLE InsuranceProvider (
    InsuranceID UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
    Name VARCHAR(255) NOT NULL
);

-- Medication Table
CREATE TABLE Medication (
    MedicationID UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
    Name VARCHAR(255) NOT NULL
);

-- Test Table
CREATE TABLE Test (
    TestID UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
    Name VARCHAR(255) NOT NULL
);

-- Indexes for common queries
CREATE INDEX IX_Admission_PatientID ON Admission(PatientID);
CREATE INDEX IX_Admission_MedicalCondition ON Admission(MedicalCondition);
CREATE INDEX IX_Patient_Age ON Patient(Age);
