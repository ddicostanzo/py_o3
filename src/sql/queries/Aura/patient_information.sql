DECLARE @mrn nvarchar;

SELECT

dp.PatientFullAddress,
pat.ClinicalTrialFlag,
dp.PatientLanguage,
pat.SpecialNeeds,
pat_add.PostalCode

FROM DWH.FactPatient fp
INNER JOIN DWH.DimPatient dp ON dp.DimPatientID = fp.DimPatientID and dp.PatientId = @mrn
INNER JOIN AuraStaging.dbo.Patient pat ON pat.PatientSer = dp.ctrPatientSer
INNER JOIN AuraStaging.dbo.PatientAddress pa ON pa.PatientSer = pat.PatientSer and pa.PrimaryFlag = 1
INNER JOIN AuraStaging.dbo.Address pat_add ON pat_add.AddressSer = pa.AddressSer
ORDER BY dp.DimPatientID DESC;