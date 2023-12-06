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
    left join [Integration].[MasterReference].[Patient] AS inte --- to get EMPI
    on inte.EDWPatientID = pt.EDWPatientID
    left join [Integration].[EMPI].[MRN_MGH] as mrn --- to get MRN
    on mrn.EDWPatientID = pt.EDWPatientID and mrn.MRNSiteCD = 'MGH'

WHERE
    ---(ElementID = 'MGB#266' OR ElementID = 'MGB#268')
    sdedata.CurrentValueSourceSmartFormID = 5719 ---'SmartForm 5720','SmartForm 5721',
    and CurrentValueDTS between '{dynamic_start_date}' and '{dynamic_end_date}'
    AND id.IdentityTypeID = '67'
    and mrn.StatusCD = 'A'

ORDER BY
    id.PatientIdentityID
    , sdedata.CurrentValueSourceDSC