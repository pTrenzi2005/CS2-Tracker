# The CI identity and catalog in the DEV workspace. Applied locally only: the Terraform identity has no access
# to DEV, and creating a catalog needs your CREATE_CATALOG from the DEV admins group.
terraform {
  required_version = ">= 1.11"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 5.11"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.140"
    }
  }

  backend "azurerm" {
    key = "dev.tfstate"
  }
}

provider "azurerm" {
  subscription_id = var.subscription_id
  features {}
}

provider "databricks" {
  host      = var.workspace_url
  auth_type = "azure-cli"
}
