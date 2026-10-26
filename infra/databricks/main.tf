locals {
  azure        = data.terraform_remote_state.azure.outputs
  workspace_id = local.azure.workspace_id

  # Owner of the storage credential, the external locations, and the catalog. Unity Catalog rejects the system
  # group _workspace_admins_<workspace>_<id> as an owner, and only the operator can create metastore objects:
  # CREATE_STORAGE_CREDENTIAL and the rest come from the DEV admins group, which the PROD admins group lacks.
  # The Terraform identity gets MANAGE on each, so CI can maintain them.
  uc_owner = data.databricks_user.operator.user_name

  # Privileges the deployer needs on each location: schema storage everywhere, the external raw volume in bronze.
  # catalog holds only the catalog's own managed location.
  location_privileges = {
    bronze  = ["CREATE_MANAGED_STORAGE", "CREATE_EXTERNAL_VOLUME", "READ_FILES", "WRITE_FILES"]
    silver  = ["CREATE_MANAGED_STORAGE"]
    gold    = ["CREATE_MANAGED_STORAGE"]
    catalog = []
  }
}
