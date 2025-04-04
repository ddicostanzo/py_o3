SELECT

dp.PatientId,
dp.PatientLastName,
dp.PatientFirstName,
dp.PatientDateOfBirth,
Year(dp.PatientDateOfBirth) as 'YearOfBirth',
dp.PatientDeathDate,
DATEDIFF(day, dp.PatientDateOfBirth, dp.PatientDeathDate) / 365.25 as 'AgeAtDeath',
race.LookupDescriptionENU as 'Race',
pat.Sex,
dp.PatientFullAddress

FROM DWH.FactPatient fp
INNER JOIN DWH.DimPatient dp ON dp.DimPatientID = fp.DimPatientID
INNER JOIN AuraStaging.dbo.Patient pat ON pat.PatientSer = dp.ctrPatientSer
INNER JOIN DWH.DimNationality dn ON dn.DimNationalityID = fp.DimNationalityID
INNER JOIN DWH.DimLookup race ON race.DimLookupID = fp.DimLookupID_Race

ORDER BY dp.DimPatientID DESC;
