-- Patient Table
CREATE TABLE Patient (
    PatientID UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
    Name VARCHAR(255) NOT NULL,
    Age INT,
    Gender VARCHAR(10),
    BloodType VARCHAR(5)
);
-- Admission Table
CREATE TABLE Admission (
    AdmissionID UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
    DateOfAdmission DATE,
    RoomNumber INT,
    AdmissionType VARCHAR(20),
    DischargeDate DATE,
    BillingAmount FLOAT,

);
-- Doctor Table
CREATE TABLE Doctor (
    DoctorID UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
    Name VARCHAR(255)
);
-- Hospital Table
CREATE TABLE Hospital (
    HospitalID UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
    Name VARCHAR(255)
);
-- InsuranceProvider Table
CREATE TABLE InsuranceProvider (
    InsuranceID UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
    Name VARCHAR(255)
);
-- Medication Table
CREATE TABLE Medication (
    MedicationID UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
    Name VARCHAR(255)
);

-- Test Table
CREATE TABLE Test (
    TestID UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
    Name VARCHAR(255)
);