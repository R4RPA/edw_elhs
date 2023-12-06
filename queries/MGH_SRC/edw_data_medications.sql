SELECT [OrderID]
          ,med.[PatientID]
          ,med.[PatientEncounterDateRealNBR]
          ,[PatientEncounterID]
          ,[OrderDTS]
          ,[OrderClassCD]
          ,[OrderClassDSC]
          ,[PharmacyID]
          ,[OrderCreatorUserID]
          ,[MedicationID]
          ,[MedicationDSC]
          ,[SigTXT]
          ,[DoseAMT]
          ,[PrescriptionQuantityNBR]
          ,[RefillCNT]
          ,[StartDTS]
          ,[EndDTS]
          ,[DispenseAsWrittenFLG]
          ,[MedicationDiscontinueReasonCD]
          ,[MedicationDiscontinueReasonDSC]
          ,[MedicationPrescribingProviderID]
          ,[NonFormularyExceptionCD]
          ,[NonFormularyExceptionDSC]
          ,[MedicationPanelID]
          ,[UpdateDTS]
          ,[OrderInstantDTS]
          ,[MedicationDisplayNM]
          ,[OriginalMedicationID]
          ,[HospitalistOrderFLG]
          ,[OrderPriorityCD]
          ,[OrderPriorityDSC]
          ,[MedicationRouteCD]
          ,[MedicationRouteDSC]
          ,[OrderDiscontinuedUserID]
          ,[OrderDiscontinuedDTS]
          ,[OriginalMedicationOrderID]
          ,[PendedOrderApproverUserID]
          ,[PendedOrderRefusalReasonCD]
          ,[PendedOrderRefusalReasonDSC]
          ,[DiscreteFrequencyID]
          ,[DiscreteFrequencyDSC]
          ,[DiscreteDoseAMT]
          ,[HVDoseUnitCD]
          ,[HVDoseUnitDSC]
          ,[SelfAdministeredFLG]
          ,[OrderStartDTS]
          ,[OrderEndDTS]
          ,[NonFormularyFLG]
          ,med.[CommunityPhysicalOwnerID]
          ,med.[CommunityLogicalOwnerID]
          ,[OrderStatusCD]
          ,[OrderStatusDSC]
          ,[WorkstationID]
          ,[PrescribingOrAuthorizingProviderID]
          ,[OrderingProviderID]
          ,[OrderSessionReportPrintedFLG]
          ,[MinimumDoseAMT]
          ,[MaximumDoseAMT]
          ,[DoseUnitCD]
          ,[DoseUnitDSC]
          ,[OrderPendingFLG]
          ,[BulkDispenseOrderFLG]
          ,[ProviderTypeCD]
          ,[ProviderTypeDSC]
          ,[PatientLocationID]
          ,[PatientLocationDSC]
          ,[ReorderedOrModifiedCD]
          ,[ReorderedOrModifiedDSC]
          ,[SpecifiedFirstDTS]
          ,[ScheduledStartDTS]
          ,[AdditionalInformationOrderStatusCD]
          ,[AdditionalInformationOrderStatusDSC]
          ,[OrderExpirationAfterStartDTS]
          ,[OrderExpirationBeforeEndDTS]
          ,[MedicationOrderCommentTXT]
          ,[UserSelectedMedicationID]
          ,[UserSelectedMedicationDTS]
          ,[NurseVerificationRequiredFLG]
          ,[MedicationProblemListID]
          ,[LastAdministeredDoseCommentTXT]
          ,[PriorToAdmissionMedicationInformantCD]
          ,[PriorToAdmissionMedicationInformantDSC]
          ,[AmbulatoryMedicationNM]
          ,[DispenseLogicMostRecentDepartmentID]
          ,[DispenseLogicMostRecentDepartmentDSC]
          ,[DispenseLogicMostRecentCareAreaID]
          ,[CalculateRateByVolumeDurationFLG]
          ,[PatientWeightBasedDoseFLG]
          ,[ReviewPatientWeightFLG]
          ,[LastReviewedPatientWeightNBR]
          ,[ReviewPatientWeightDTS]
          ,[LastNonReviewedPatientWeightNBR]
          ,[LastNonReviewedPatientWeightDTS]
          ,[RefillRemainingCNT]
          ,[RefillOrderAuthorizingProviderID]
          ,[ParentMedicationOrderID]
          ,[ParentMedicationOrderDateRealNBR]
          ,[RulesBasedOrderTransmittalFLG]
          ,[ResumeStatusCD]
          ,[ResumeStatusDSC]
          ,[AuthorizingProviderID]
          ,[LoginDepartmentID]
          ,[LoginDepartmentDSC]
          ,[SessionKeyID]
          ,[OrderingModeCD]
          ,[OrderingModeDSC]
          ,[PendingMedicationApprovalStatusCD]
          ,[PendingMedicationApprovalStatusDSC]
          ,[ProviderStatusCD]
          ,[ProviderStatusDSC]
          ,[PharmacistVerifiedNonFormularyFLG]
          ,[ExternalEligibilitySourceID]
          ,[ExternalEligibilityMemberID]
          ,[ExternalFormularyID]
          ,[ExternalCoverageID]
          ,[ExternalCopayID]
          ,[ExternalPharmacyTypeCD]
          ,[ExternalPharmacyTypeDSC]
          ,[ExternalFormularyStatusCD]
          ,[ExternalCoverageAgeLimitFLG]
          ,[ExternalCoverageExclusionFLG]
          ,[ExternalCoverageGenderLimitFLG]
          ,[ExternalCoverageMedicalNecessityFLG]
          ,[ExternalCoveragePriorAuthorizationFLG]
          ,[ExternalCoverageQuantityLimitFLG]
          ,[ExternalCoverageResourceLinkDrugFLG]
          ,[ExternalCoverageResourceLinkSummaryFLG]
          ,[ExternalCoverageStepMedicationFLG]
          ,[ExternalCoverageStepTherapyFLG]
          ,[ExternalCoverageTextMessageFLG]
          ,[UserSelectedIMSFLG]
          ,[IndicationCommentTXT]
          ,[DoseAdjustmentTypeCD]
          ,[DoseAdjustmentTypeDSC]
          ,[DoseAdjustmentOverriddenFLG]
          ,[MaximumAllowedDoseAMT]
          ,[MaximumAllowedDoseUnitCD]
          ,[MaximumAllowedDoseUnitDSC]
          ,[PRNCommentTXT]
          ,[OrderUpdateDTS]
          ,[MedicationReorderMethodCD]
          ,[MedicationReorderMethodDSC]
          ,[DiscreteDispenseQuantityNBR]
          ,[DiscreteDispenseUnitCD]
          ,[DiscreteDispenseUnitDSC]
          ,[CompletedOrderDefaultDTS]
          ,[BSABasedDoseFLG]
          ,[BSAReviewFLG]
          ,[BSALastReviewedNBR]
          ,[BSALastNonReviewedNBR]
          ,[LastDoseTimeTXT]
          ,med.[EDWLastModifiedDTS]
      FROM [Epic].[Orders].[Medication_MGH] med

    where PatientID in ({dynamic_patient_ids})

    and (med.MedicationDisplayNM like ('%Acetazolamide%')
            or med.MedicationDisplayNM like ('%ACTH%')
            or med.MedicationDisplayNM like ('%Brivaracetam%')
            or med.MedicationDisplayNM like ('%Carbamazepine%')
            or med.MedicationDisplayNM like ('%Cannabidiol%')
            or med.MedicationDisplayNM like ('%Cenobamate%')
            or med.MedicationDisplayNM like ('%Clobazam%')
            or med.MedicationDisplayNM like ('%Clonazepam%')
            or med.MedicationDisplayNM like ('%Clorazepate%')
            or med.MedicationDisplayNM like ('%Diazepam%')
            or med.MedicationDisplayNM like ('%Divalproex%')
            or med.MedicationDisplayNM like ('%Eslicarbazepine%')
            or med.MedicationDisplayNM like ('%Ethosuximide%')
            or med.MedicationDisplayNM like ('%Ezogabine%')
            or med.MedicationDisplayNM like ('%Felbamate%')
            or med.MedicationDisplayNM like ('%Gabapentin%')
            or med.MedicationDisplayNM like ('%Lacosamide%')
            or med.MedicationDisplayNM like ('%Lamotrigine%')
            or med.MedicationDisplayNM like ('%Levetiracetam%')
            or med.MedicationDisplayNM like ('%Levetiracetam ER%')
            or med.MedicationDisplayNM like ('%Lorazepam%')
            or med.MedicationDisplayNM like ('%Methsuximide%')
            or med.MedicationDisplayNM like ('%Oxcarbazepine%')
            or med.MedicationDisplayNM like ('%Perampanel%')
            or med.MedicationDisplayNM like ('%Phenobarbital%')
            or med.MedicationDisplayNM like ('%Phenytoin%')
            or med.MedicationDisplayNM like ('%Pregabalin%')
            or med.MedicationDisplayNM like ('%Primidone%')
            or med.MedicationDisplayNM like ('%Rufinamide%')
            or med.MedicationDisplayNM like ('%Stiripentol%')
            or med.MedicationDisplayNM like ('%Tiagabine%')
            or med.MedicationDisplayNM like ('%Topiramate%')
            or med.MedicationDisplayNM like ('%Valproic acid%')
            or med.MedicationDisplayNM like ('%Vigabatrin%')
            or med.MedicationDisplayNM like ('%Zonisamide')
        )