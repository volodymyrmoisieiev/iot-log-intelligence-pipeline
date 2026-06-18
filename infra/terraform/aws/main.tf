locals {
  common_tags = {
    project     = var.project_name
    environment = var.environment
    managed_by  = "terraform"
  }
}

# Stage 12A keeps this root module intentionally empty.
# Real AWS resources will be added in later stages.
