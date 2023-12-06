SELECT
    max(CurrentValueDTS) as MaxDate

    FROM
    Epic.Encounter.SmartDataElementValue_MGH AS sf_val
    INNER JOIN
    Epic.Encounter.SmartDataElementData_MGH AS sdedata
    ON sf_val.ConceptValueID = sdedata.ConceptValueID
    INNER JOIN
    Epic.Patient.Identity_MGH AS id
    ON id.PatientID = sdedata.PatientLinkID
    left join
    Epic.Reference.ClarityConcept as fields
    on sdedata.ElementID = fields.ConceptID
    left join [Epic].[Person].[Employee_MGH] emp
    ON sdedata.UserID = emp.UserID
    left join [Epic].[Patient].[Patient_MGH] AS pt
    on id.PatientID = pt.PatientID
    left join [Integration].[MasterReference].[Patient] AS inte 
    on inte.EDWPatientID = pt.EDWPatientID
    left join [Integration].[EMPI].[MRN_MGH] as mrn
    on mrn.EDWPatientID = pt.EDWPatientID and mrn.MRNSiteCD = 'MGH'

    WHERE
    sdedata.CurrentValueSourceSmartFormID = 5719 
    and CurrentValueDTS >= '2019-01-01'
    AND id.IdentityTypeID = '67'
    and mrn.StatusCD = 'A'

    