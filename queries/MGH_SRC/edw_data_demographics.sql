select distinct
        b.PatientID
        ,PID.PatientIdentityID as [MRN]
        ,b.[BirthDTS]
        ,b.[EthnicGroupDSC]
        ,a.MaritalStatusDSC
        ,b.[ReligionDSC]
        ,a.PrimaryLanguageNM
        ,replace(PatientLastNM,',','') as PatientLastNM
        ,replace(PatientFirstNM,',','') as PatientFirstNM
        ,b.[ZipCD]
        ,replace([AddressLine01TXT],',','') as Address1
        ,replace([AddressLine02TXT],',','') as Address2
        ,b.[CityNM]
        ,[StateDSC]
        ,[CountyDSC]
        ,[CountryDSC]
        ,a.GenderIdentityTXT
        ,[SexDSC]
        ,a.EducationLevelDSC
        ,a.SexAtBirthTXT as SexAssignedAtBirthDSC
        ,a.Race01DSC
        ,b.[EmploymentStatusDSC]
        ,PSO.[SexualOrientationDSC]
        ,p4.GenderIdentityDSC
        ,appmt.[AppointmentDTS]
        ,appmt.[PatientEncounterID]
        ,appmt.[ProviderNameWithIDNM]
        ,Appmt.[VisitTypeDSC]

        from integration.empi.patient_mgh a
        join  [Epic].[Patient].[Patient_MGH] b on b.EDWPatientID = a.EDWPatientID
        inner join EPIC.Patient.Identity_MGH PID on PID.PatientID = b.PatientID
        left join [Epic].[Encounter].[AppointmentDetail_MGH] appmt on appmt.PatientID = b.PatientID
        left join [Epic].[Patient].[SexualOrientation_MGH] PSO on b.PatientID = PSO.PatientID
        left join Epic.Patient.Patient4_MGH p4 on b.PatientID = p4.PatientID

        where  IdentityTypeID = 67
        and appmt.DepartmentNM = 'MGP EPILEPSY UN WAC8'
        and (appmt.AppointmentDTS between '{dynamic_start_date}' and '{dynamic_end_date}')
        and appmt.AppointmentStatusDSC = 'Completed'
        and a.StatusCD = 'A'
        and (PSO.LineNBR = 1 OR PSO.LineNBR IS NULL)