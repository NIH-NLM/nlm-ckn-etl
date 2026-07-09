gget_queries = {
    "target": {
        "purpose": "Obtain target attributes",
        "variables": {
            "ensemblId": "ENSG00000169252",
        },
        "query_string": """
query target($ensemblId: String!) {
  target(ensemblId: $ensemblId) {
    id
    dbXrefs {
        id
        source
    }
    proteinIds {
      id
      source
    }
    transcriptIds
    approvedSymbol
    approvedName
    associatedDiseases {
      count
      rows {
        score
        disease {
          id
          description
          dbXRefs
          name
        }
      }
    }
    drugAndClinicalCandidates {
      count
      rows {
        drug {
          id
          name
          description
          maximumClinicalStage
          synonyms {
            label
          }
          tradeNames {
            label
          }
          indications {
            count
            rows {
              disease {
                id
                name
                description
              }
              maxClinicalStage
              clinicalReports {
                source
                id
              }
            }
          }
          drugType
          mechanismsOfAction {
            rows {
              mechanismOfAction
              targets {
                id
              }
              targetName
            }
          }
          drugWarnings {
            efoId
            efoTerm
            efoIdForWarningClass
            description
            warningType
            year
          }
        }
      }
    }
    pharmacogenomics {
      variantRsId
      genotypeId
      genotype
      variantFunctionalConsequenceId
      variantFunctionalConsequence {
        label
      }
      drugs {
        drugId
        drugFromSource
        drug {
          name
        }
      }
      phenotypeText
      genotypeAnnotationText
      pgxCategory
      isDirectTarget
      evidenceLevel
      datasourceId
      literature
      haplotypeFromSourceId
      targetFromSourceId
      studyId
      datatypeId
      phenotypeFromSourceId
      variantId
      haplotypeId
    }
  }
}
""",
    },
    "diseases": {
        "purpose": "Duplicate and extend gget opentagets -r diseases command",
        "variables": {
            "ensemblId": "ENSG00000169252",
        },
        "query_string": """
query diseases($ensemblId: String!) {
  target(ensemblId: $ensemblId) {
    id
    associatedDiseases {
      count
      rows {
        score
        disease {
          id
          description
          dbXRefs
          name
        }
      }
    }
  }
}
""",
    },
    "drugs": {
        "purpose": "Update and extend gget opentagets -r drugs command",
        "variables": {
            "ensemblId": "ENSG00000169252",
        },
        "query_string": """
query diseases($ensemblId: String!) {
  target(ensemblId: $ensemblId) {
    id
    drugAndClinicalCandidates {
      count
      rows {
        drug {
          id
          description
          maximumClinicalStage
          synonyms
          tradeNames
          name
          indications {
            count
            rows {
              disease {
                id
                name
                description
              }
              maxClinicalStage
              clinicalReports {
                source
                id
              }
            }
          }
          drugWarnings {
            efoId
            efoTerm
            efoIdForWarningClass
            description
            warningType
            year
          }
        }
      }
    }
  }
}
""",
    },
    "pharmacogenetics": {
        "purpose": "Duplicate and extend gget opentagets -r pharmacogenetics command",
        "variables": {
            "ensemblId": "ENSG00000169252",
        },
        "query_string": """
query pharmacogenetics($ensemblId: String!) {
  target(ensemblId: $ensemblId) {
    id
    pharmacogenomics {
      variantRsId
      genotypeId
      genotype
      variantFunctionalConsequenceId
      variantFunctionalConsequence {
        label
      }
      drugs {
        drugId
        drugFromSource
        drug {
          name
        }
      }
      phenotypeText
      genotypeAnnotationText
      pgxCategory
      isDirectTarget
      evidenceLevel
      datasourceId
      literature
      haplotypeFromSourceId
      targetFromSourceId
      studyId
      datatypeId
      phenotypeFromSourceId
      variantId
      haplotypeId
    }
  }
}
""",
    },
}
