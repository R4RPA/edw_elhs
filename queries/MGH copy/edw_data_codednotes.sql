SELECT distinct
       Note.PatientID,
       Note.PatientEncounterID,
       PID.PatientIdentityID as MRN
      ,Nenc.[ContactDateRealNBR]
      ,nTXT.LineNBR
      ,nTXT.NoteID
      ,penc.DepartmentDSC
      ,replace(emp.UserNM,',','') as Author
      ,replace(nTXT.NoteTXT,',','') as Notes
FROM [Epic].[Clinical].[NoteEncounterInformation_MGH] Nenc
    JOIN EPIC.Clinical.Note_MGH Note on Note.NoteID = Nenc.NoteID
    JOIN EPIC.Patient.Identity_MGH PID on PID.PatientID = Note.PatientID
    JOIN Epic.Clinical.NoteText_MGH nTXT on nTXT.NoteCSNID = Nenc.CSNID and nTXT.NoteID = Nenc.NoteID
    JOIN Epic.Encounter.PatientEncounter_MGH Penc on Penc.PatientEncounterID = Note.PatientEncounterID
    JOIN [Epic].[Person].[Employee_MGH] emp ON emp.ProviderID = Nenc.AuthorLinkedProviderID

where PID.IdentityTypeID = 67
    AND Penc.DepartmentDSC = 'MGP EPILEPSY UN WAC8' ---OR DepartmentDSC = 'BWH NEURO EPILEP' OR DepartmentDSC = 'WHP NEURO EX9')
    and Penc.AppointmentStatusDSC = 'Completed'
    and (Penc.AppointmentDTS between '{dynamic_start_date}' and '{dynamic_end_date}')