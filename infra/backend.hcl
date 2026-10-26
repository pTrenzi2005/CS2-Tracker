# Shared backend settings. Each stack sets its own key. infra/bootstrap/bootstrap.ps1 creates the account.
# terraform -chdir=infra/<stack> init -backend-config="../backend.hcl"
subscription_id      = "3fc38270-4a82-4fa3-8e33-99c47ac04c9e"
resource_group_name  = "rg-cs2-tfstate"
storage_account_name = "stcs2trackertfstate"
container_name       = "tfstate"
use_azuread_auth     = true
