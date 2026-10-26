locals {
  tags = {
    project    = "cs2-tracker"
    env        = "prod"
    managed_by = "terraform"
  }

  # Unity Catalog storage. The bundle puts schema storage under <layer>/uc and the raw volume under bronze/raw.
  # catalog holds the catalog's own managed location.
  containers = toset(["bronze", "silver", "gold", "catalog"])
}

data "azurerm_resource_group" "prod" {
  name = var.resource_group_name
}

# azurerm has no computeMode, so the workspace goes through the ARM API. Serverless like DEV: no managed
# resource group, VNet, or NAT gateway, and no classic clusters to count against the subscription's vCPU quota.
resource "azapi_resource" "workspace" {
  type      = "Microsoft.Databricks/workspaces@2026-01-01"
  name      = var.workspace_name
  parent_id = data.azurerm_resource_group.prod.id
  location  = var.location
  tags      = local.tags

  body = {
    sku = {
      name = "premium"
    }
    properties = {
      computeMode         = "Serverless"
      publicNetworkAccess = "Enabled"
    }
  }

  response_export_values = ["properties.workspaceId", "properties.workspaceUrl"]

  lifecycle {
    prevent_destroy = true
  }
}

resource "azurerm_storage_account" "lake" {
  name                            = var.storage_account_name
  resource_group_name             = data.azurerm_resource_group.prod.name
  location                        = var.location
  account_kind                    = "StorageV2"
  account_tier                    = "Standard"
  account_replication_type        = "LRS"
  is_hns_enabled                  = true
  min_tls_version                 = "TLS1_2"
  allow_nested_items_to_be_public = false
  tags                            = local.tags

  blob_properties {
    delete_retention_policy {
      days = 7
    }
    container_delete_retention_policy {
      days = 7
    }
  }

  lifecycle {
    prevent_destroy = true
  }
}

resource "azurerm_storage_container" "lake" {
  for_each           = local.containers
  name               = each.key
  storage_account_id = azurerm_storage_account.lake.id
}

resource "azurerm_databricks_access_connector" "unity" {
  name                = var.access_connector_name
  resource_group_name = data.azurerm_resource_group.prod.name
  location            = var.location
  tags                = local.tags

  identity {
    type = "SystemAssigned"
  }
}

resource "azurerm_role_assignment" "unity_storage" {
  scope                = azurerm_storage_account.lake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.unity.identity[0].principal_id
  principal_type       = "ServicePrincipal"
}
