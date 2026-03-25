-- InsureYours Schema — MySQL 8.0+
-- Run this FIRST before any other script.
-- Idempotent: safe to re-run.

CREATE DATABASE IF NOT EXISTS Healthcare_Group_Project;
USE Healthcare_Group_Project;

-- Disable FK checks so tables can be dropped in any order on re-run
SET FOREIGN_KEY_CHECKS = 0;

-- ============================================================
-- Staging table (flat CSV mirror — ETL loads here first)
-- ============================================================
DROP TABLE IF EXISTS HealthCare_Dataset;
CREATE TABLE HealthCare_Dataset (
    Name              VARCHAR(255),
    Age               INT,
    Gender            VARCHAR(10),
    BloodType         VARCHAR(5),
    MedicalCondition  VARCHAR(50),
    DateOfAdmission   DATE,
    Doctor            VARCHAR(255),
    Hospital          VARCHAR(255),
    InsuranceProvider  VARCHAR(255),
    BillingAmount     DECIMAL(18,4),
    RoomNumber        INT,
    AdmissionType     VARCHAR(20),
    DischargeDate     DATE,
    Medication        VARCHAR(255),
    TestResults       VARCHAR(20)
);

-- ============================================================
-- Dimension tables (created before Admission for FK integrity)
-- ============================================================
DROP TABLE IF EXISTS Admission;  -- drop fact first (has FKs)

DROP TABLE IF EXISTS Patient;
CREATE TABLE Patient (
    PatientID     INT AUTO_INCREMENT PRIMARY KEY,
    Name          VARCHAR(255) NOT NULL,
    Age           INT CHECK (Age >= 0 AND Age <= 150),
    Gender        VARCHAR(10) CHECK (Gender IN ('Male', 'Female')),
    BloodType     VARCHAR(5)  CHECK (BloodType IN ('A+','A-','B+','B-','AB+','AB-','O+','O-'))
);

DROP TABLE IF EXISTS Doctor;
CREATE TABLE Doctor (
    DoctorID INT AUTO_INCREMENT PRIMARY KEY,
    Name     VARCHAR(255) NOT NULL
);

DROP TABLE IF EXISTS Hospital;
CREATE TABLE Hospital (
    HospitalID INT AUTO_INCREMENT PRIMARY KEY,
    Name       VARCHAR(255) NOT NULL
);

DROP TABLE IF EXISTS InsuranceProvider;
CREATE TABLE InsuranceProvider (
    InsuranceID INT AUTO_INCREMENT PRIMARY KEY,
    Name        VARCHAR(255) NOT NULL
);

DROP TABLE IF EXISTS Medication;
CREATE TABLE Medication (
    MedicationID INT AUTO_INCREMENT PRIMARY KEY,
    Name         VARCHAR(255) NOT NULL
);

-- ============================================================
-- Fact table (all FKs reference tables created above)
-- ============================================================
CREATE TABLE Admission (
    AdmissionID      INT AUTO_INCREMENT PRIMARY KEY,
    PatientID        INT NOT NULL,
    DoctorID         INT NOT NULL,
    HospitalID       INT NOT NULL,
    InsuranceID      INT NOT NULL,
    MedicationID     INT NOT NULL,
    DateOfAdmission  DATE,
    RoomNumber       INT,
    AdmissionType    VARCHAR(20) CHECK (AdmissionType IN ('Elective', 'Emergency', 'Urgent')),
    DischargeDate    DATE,
    BillingAmount    DECIMAL(18,4) CHECK (BillingAmount >= 0),
    MedicalCondition VARCHAR(50) CHECK (MedicalCondition IN (
        'Diabetes', 'Asthma', 'Obesity', 'Arthritis', 'Hypertension', 'Cancer'
    )),
    TestResults      VARCHAR(20) CHECK (TestResults IN ('Normal', 'Abnormal', 'Inconclusive')),
    CONSTRAINT chk_discharge_after_admission
        CHECK (DischargeDate IS NULL OR DischargeDate >= DateOfAdmission),
    FOREIGN KEY (PatientID)    REFERENCES Patient(PatientID),
    FOREIGN KEY (DoctorID)     REFERENCES Doctor(DoctorID),
    FOREIGN KEY (HospitalID)   REFERENCES Hospital(HospitalID),
    FOREIGN KEY (InsuranceID)  REFERENCES InsuranceProvider(InsuranceID),
    FOREIGN KEY (MedicationID) REFERENCES Medication(MedicationID)
);

-- ============================================================
-- Output table for insurance ranking analysis
-- ============================================================
DROP TABLE IF EXISTS Health_Data_Analysis;
CREATE TABLE Health_Data_Analysis (
    AgeGroup          VARCHAR(10),
    BloodType         VARCHAR(5),
    MedicalCondition  VARCHAR(50),
    Medication        VARCHAR(255),
    InsuranceProvider  VARCHAR(255),
    AvgBillingAmount  DECIMAL(18,4),
    MinBillingAmount  DECIMAL(18,4),
    MaxBillingAmount  DECIMAL(18,4),
    ClaimCount        INT,
    ProviderRank      INT
);

-- ============================================================
-- Indexes for analytical and FK join queries
-- ============================================================
CREATE INDEX idx_admission_patient    ON Admission(PatientID);
CREATE INDEX idx_admission_doctor     ON Admission(DoctorID);
CREATE INDEX idx_admission_hospital   ON Admission(HospitalID);
CREATE INDEX idx_admission_insurance  ON Admission(InsuranceID);
CREATE INDEX idx_admission_medication ON Admission(MedicationID);
CREATE INDEX idx_admission_condition  ON Admission(MedicalCondition);
CREATE INDEX idx_admission_type       ON Admission(AdmissionType);
CREATE INDEX idx_patient_age          ON Patient(Age);
CREATE INDEX idx_patient_bloodtype    ON Patient(BloodType);

-- Staging table indexes for ETL lookups
CREATE INDEX idx_staging_doctor    ON HealthCare_Dataset(Doctor);
CREATE INDEX idx_staging_hospital  ON HealthCare_Dataset(Hospital);
CREATE INDEX idx_staging_insurance ON HealthCare_Dataset(InsuranceProvider);
CREATE INDEX idx_staging_medication ON HealthCare_Dataset(Medication);

-- Re-enable FK checks
SET FOREIGN_KEY_CHECKS = 1;
