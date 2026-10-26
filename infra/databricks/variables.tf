variable "subscription_id" {
  description = "Azure subscription that holds the Terraform state."
  type        = string
  default     = "3fc38270-4a82-4fa3-8e33-99c47ac04c9e"
}

variable "state" {
  description = "Backend of infra/azure. Same values as infra/backend.hcl."
  type = object({
    resource_group_name  = string
    storage_account_name = string
    container_name       = string
  })
  default = {
    resource_group_name  = "rg-cs2-tfstate"
    storage_account_name = "stcs2trackertfstate"
    container_name       = "tfstate"
  }
}

variable "catalog_name" {
  description = "PROD catalog. Matches var.catalog of the bundle's prod target."
  type        = string
  default     = "cs2_prod"
}

variable "operator_user_name" {
  description = "Person who runs PROD jobs and reads every layer. Signs in to the workspace before the first apply."
  type        = string
  default     = "226190@student.uek.krakow.pl"
}

variable "secret_scope" {
  description = "Matches var.secret_scope of the bundle."
  type        = string
  default     = "cs2tracker-secrets"
}

variable "secret_key" {
  description = "Matches var.secret_key of the bundle."
  type        = string
  default     = "evh-connection-string"
}
