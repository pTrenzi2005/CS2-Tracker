variable "subscription_id" {
  description = "Azure subscription shared by DEV and PROD."
  type        = string
  default     = "3fc38270-4a82-4fa3-8e33-99c47ac04c9e"
}

variable "location" {
  description = "Region of the DEV resources."
  type        = string
  default     = "swedencentral"
}

variable "resource_group_name" {
  description = "DEV resource group: workspace, storage account, Event Hub."
  type        = string
  default     = "CS2-Tracker-Project"
}

variable "github_subject_repository" {
  description = "Repository part of the GitHub OIDC subject: owner@owner_id/name@repository_id. GitHub puts the IDs in, so a renamed or recreated repository cannot sign in."
  type        = string
  default     = "pTrenzi2005@202133242/CS2-Tracker@1368773264"
}

variable "workspace_url" {
  description = "DEV workspace. Same host as the bundle's dev and ci targets."
  type        = string
  default     = "https://adb-7405619783206256.16.azuredatabricks.net"
}

variable "workspace_id" {
  description = "DEV workspace ID, for the catalog binding."
  type        = string
  default     = "7405619783206256"
}

variable "storage_account_name" {
  description = "DEV ADLS Gen2 account. Its containers are the external locations below."
  type        = string
  default     = "datalakecstracker"
}

variable "catalog_name" {
  description = "Matches var.catalog of the bundle's ci target."
  type        = string
  default     = "cs2_ci"
}
