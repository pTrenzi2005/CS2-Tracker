terraform {
  required_version = ">= 1.11"

  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.140"
    }
  }

  backend "azurerm" {
    key = "databricks.tfstate"
  }
}

data "terraform_remote_state" "azure" {
  backend = "azurerm"
  config = {
    subscription_id      = var.subscription_id
    resource_group_name  = var.state.resource_group_name
    storage_account_name = var.state.storage_account_name
    container_name       = var.state.container_name
    key                  = "azure.tfstate"
    use_azuread_auth     = true
  }
}

# Locally the token comes from az login. In GitHub Actions it comes from azure/login with the Terraform identity,
# which is a workspace admin through Contributor on the resource group.
provider "databricks" {
  host                        = data.terraform_remote_state.azure.outputs.workspace_url
  azure_workspace_resource_id = data.terraform_remote_state.azure.outputs.workspace_resource_id
  auth_type                   = "azure-cli"
}
