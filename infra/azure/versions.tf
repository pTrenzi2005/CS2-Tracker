terraform {
  required_version = ">= 1.11"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 5.11"
    }
    azapi = {
      source  = "azure/azapi"
      version = "~> 2.13"
    }
  }

  backend "azurerm" {
    key = "azure.tfstate"
  }
}

provider "azurerm" {
  subscription_id = var.subscription_id
  features {}
}

provider "azapi" {
  subscription_id = var.subscription_id
}
