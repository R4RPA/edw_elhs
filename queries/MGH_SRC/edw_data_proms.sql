select distinct inte.EMPI, pt.EDWPatientID, qs.PatientID, mrn.MRN,
    pt.SexDSC, pt.BirthDTS, pt.LanguageDSC,
    appmt.AppointmentDTS, appmt.AppointmentStatusDSC,appmt.DepartmentNM, appmt.DepartmentSpecialtyDSC, appmt.ProviderNameWithIDNM,
    qs.AnswerID,
    qfa.PatientEncounterID,
    qs.SubmissionDTS,
    qfa.FormID as RootQuestionnaireSetID,
    qfr.FormNM as RootQuestionnaireSetNM,
    qs.QuestionnaireID,
    qf.FormNM as IndividualQuestionnaireNM,
    qq.QuestionID,
    qq.QuestionNM,
    qq.QuestionTypeDSC,
    qqc.QuestionTXT,
    qqa.AnswerTXT,
    pay.PayorNM


from (select AnswerID,PatientID,QuestionnaireID,min(SubmissionDTS) as SubmissionDTS from Epic.Patient.QuestionnaireSubmission_MGH group by AnswerID,PatientID,QuestionnaireID) qs
    inner join Epic.Clinical.QuestionnaireFormAnswer_MGH qfa on qs.AnswerID = qfa.AnswerID
    inner join Epic.Reference.QuestionnaireQuestionAnswer qqa on qs.AnswerID = qqa.AnswerID
    inner join Epic.Reference.QuestionnaireQuestion qq on qqa.QuestionID = qq.QuestionID
    inner join Epic.Reference.QuestionnaireForm qf on qs.QuestionnaireID = qf.FormID
    inner join Epic.Reference.QuestionnaireForm qfr on qfa.FormID = qfr.FormID
    inner join (select QuestionID, max(ContactDateRealNBR) as ContactDateRealNBR from Epic.Clinical.QuestionnaireQuestionContact_MGH group by QuestionID) cqc on qqa.QuestionID = cqc.QuestionID
    inner join Epic.Clinical.QuestionnaireQuestionContact_MGH qqc on qqa.QuestionID = qqc.QuestionID
    and cqc.ContactDateRealNBR = qqc.ContactDateRealNBR
    left join [Epic].[Encounter].[AppointmentDetail_MGH] AS appmt
    on qfa.PatientEncounterID = appmt.PatientEncounterID
    LEFT JOIN [Epic].[Patient].[Patient_MGH] AS pt
    on qs.PatientID = pt.PatientID

left join [Integration].[MasterReference].[Patient] AS inte --- to get EMPI
  on inte.EDWPatientID = pt.EDWPatientID
  left join [Integration].[EMPI].[MRN_MGH] as mrn --- to get MRN
  on mrn.EDWPatientID = pt.EDWPatientID and mrn.MRNSiteCD = 'MGH' --- filter the table mrn before joining
  left join [Epic].[Encounter].[PatientEncounter_MGH] AS pe --- to get payor
  on pe.PatientEncounterID = qfa.PatientEncounterID
  left join Epic.Finance.HospitalAccount_MGH AS ha
  on pe.HospitalAccountID = ha.HospitalAccountID
  left join [Epic].[Reference].[Payor] AS pay
  on pay.PayorID = ha.PrimaryPayorID


where qq.QuestionTypeCD <> '101'
    and (appmt.AppointmentDTS between '{dynamic_start_date}' and '{dynamic_end_date}')
    and appmt.AppointmentStatusDSC = 'Completed'
    and qfa.PatientEncounterID IS NOT NULL
    and appmt.DepartmentNM = 'MGP EPILEPSY UN WAC8'
    and qfr.FormNM LIKE '%PROMS MGH NEUROLOGY%'
    and (appmt.DepartmentSpecialtyDSC = 'Neurology' or appmt.DepartmentSpecialtyDSC = 'Sleep Medicine')
    and qq.QuestionID IN ('131497','132773','132783','132785','132784','132768','132779','132774','132790',
        '132776','132775','132777','132780','132771','132782','132786','132778','132772','132781','132889','132765',
        '132884','132762','132766','132759','132767','132758','132886','132888','132756','132751','132753','132890',
        '132763','132764','132752','132755','132760','132757','132761','132003','132001','132002','132000','132006',
        '132005','132004','131494','131495','131496','131497','160078','131498','160076','160077','160078','160079',
        '140348','140347','140346','160079','160080','160068','160069','160070','160071','160072','160073','160074',
        '160075','160454','121734','110966','110967','110968','110969','110970','110971','110972','113006','121735',
        '1400001400','1400001401','1400001402','1400001403','1400001404','1400001405','1400001406','1400001407',
        '1400001408','127106','1400001788','1400001787','1400001768','1400001774','1400001769','1400001770',
        '1400001771','1400001772','1400001776','1400001773','1400001777','1400001775','138967','128369','128351',
        '128350','128352','139165','127832','127834','145052','127833')