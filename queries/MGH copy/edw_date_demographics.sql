select
    max(appmt.AppointmentDTS) as MaxDate

from integration.empi.patient_mgh a
    join  [Epic].[Patient].[Patient_MGH] b on b.EDWPatientID = a.EDWPatientID
    inner join EPIC.Patient.Identity_MGH PID on PID.PatientID = b.PatientID
    left join [Epic].[Encounter].[AppointmentDetail_MGH] appmt on appmt.PatientID = b.PatientID
    left join [Epic].[Patient].[SexualOrientation_MGH] PSO on b.PatientID = PSO.PatientID
    left join Epic.Patient.Patient4_MGH p4 on b.PatientID = p4.PatientID

where  IdentityTypeID = 67
    and appmt.DepartmentNM = 'MGP EPILEPSY UN WAC8'
    and appmt.AppointmentStatusDSC = 'Completed'
    and a.StatusCD = 'A'
    and (PSO.LineNBR = 1 OR PSO.LineNBR IS NULL)