variable "subscription_id" {
  description = "Azure subscription shared by DEV and PROD."
  type        = string
  default     = "3fc38270-4a82-4fa3-8e33-99c47ac04c9e"
}

variable "location" {
  description = "Region of DEV, the Event Hub, and the Unity Catalog metastore."
  type        = string
  default     = "swedencentral"
}

variable "github_subject_repository" {
  description = "Repository part of the GitHub OIDC subject: owner@owner_id/name@repository_id. GitHub puts the IDs in, so a renamed or recreated repository cannot sign in."
  type        = string
  default     = "pTrenzi2005@202133242/CS2-Tracker@1368773264"
}

variable "resource_group_name" {
  description = "PROD resource group. infra/bootstrap/bootstrap.ps1 creates it, so the Terraform identity can be scoped to it."
  type        = string
  default     = "rg-cs2-prod"
}

variable "workspace_name" {
  description = "PROD workspace. Not cs2-prod: a new workspace gets a catalog named after it, which would take cs2_prod."
  type        = string
  default     = "dbw-cs2-prod"
}

variable "storage_account_name" {
  description = "ADLS Gen2 account for the PROD catalog. Globally unique."
  type        = string
  default     = "stcs2trackerprod"
}

variable "access_connector_name" {
  description = "Access connector whose managed identity Unity Catalog uses for the PROD storage account."
  type        = string
  default     = "ac-cs2-prod"
}

variable "terraform_identity" {
  description = "Managed identity that runs Terraform from GitHub Actions. Created by infra/bootstrap/bootstrap.ps1."
  type = object({
    name                = string
    resource_group_name = string
  })
  default = {
    name                = "id-cs2-terraform"
    resource_group_name = "rg-cs2-tfstate"
  }
}

variable "eventhub" {
  description = "The Event Hub DEV already reads. PROD reads it too, through its own listen rule."
  type = object({
    resource_group_name = string
    namespace_name      = string
    name                = string
  })
  default = {
    resource_group_name = "CS2-Tracker-Project"
    namespace_name      = "evhcs2tracker"
    name                = "cs2tracker_evh"
  }
}
