SELECT
    max(Penc.AppointmentDTS) as MaxDate
    FROM [Epic].[Clinical].[NoteEncounterInformation_MGH] Nenc
    JOIN EPIC.Clinical.Note_MGH Note on Note.NoteID = Nenc.NoteID
    JOIN EPIC.Patient.Identity_MGH PID on PID.PatientID = Note.PatientID
    JOIN Epic.Clinical.NoteText_MGH nTXT on nTXT.NoteCSNID = Nenc.CSNID and nTXT.NoteID = Nenc.NoteID
    JOIN Epic.Encounter.PatientEncounter_MGH Penc on Penc.PatientEncounterID = Note.PatientEncounterID
    JOIN [Epic].[Person].[Employee_MGH] emp ON emp.ProviderID = Nenc.AuthorLinkedProviderID

    where PID.IdentityTypeID = 67
    and Penc.AppointmentDTS >= '2019-01-01'
    AND Penc.DepartmentDSC = 'MGP EPILEPSY UN WAC8'
    and Penc.AppointmentStatusDSC = 'Completed'