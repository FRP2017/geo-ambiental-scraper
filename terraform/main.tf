provider "google" {
  project = var.project_id
  region  = var.region
}

# 1. ACTIVACIÓN DE APIS
resource "google_project_service" "services" {
  for_each = toset([
    "run.googleapis.com",
    "artifactregistry.googleapis.com",
    "cloudbuild.googleapis.com",
    "storage.googleapis.com",
    "bigquery.googleapis.com"
  ])
  service            = each.key
  disable_on_destroy = false
}

# 2. PAUSA DE SEGURIDAD PARA APIS (60 seg)
resource "time_sleep" "wait_60_seconds_api" {
  depends_on = [google_project_service.services]
  create_duration = "60s"
}

# 3. RECURSOS BASE
resource "google_artifact_registry_repository" "repo" {
  location      = var.region
  repository_id = var.repository_name
  format        = "DOCKER"
  depends_on    = [time_sleep.wait_60_seconds_api]
}

resource "google_storage_bucket" "almacen" {
  name                        = var.bucket_name
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true
  depends_on                  = [time_sleep.wait_60_seconds_api]
}

# 4. CONSTRUCCIÓN AUTOMÁTICA DE LA IMAGEN
resource "null_resource" "build_and_push_image" {
  triggers = {
    dir_hash = sha1(join("", [for f in fileset("../app", "**") : filesha1("../app/${f}")]))
  }

  provisioner "local-exec" {
    command = <<EOT
      gcloud builds submit ../app --tag ${var.region}-docker.pkg.dev/${var.project_id}/${var.repository_name}/streamlit-app:latest --project ${var.project_id}
    EOT
  }

  depends_on = [
    google_artifact_registry_repository.repo,
    time_sleep.wait_60_seconds_api
  ]
}

# 5. CUENTA DE SERVICIO Y PERMISOS
resource "google_service_account" "app_sa" {
  account_id   = "crawler-sa"
  display_name = "SA para Crawler SEIA"
  depends_on   = [time_sleep.wait_60_seconds_api]
}

resource "google_project_iam_member" "perms" {
  for_each = toset([
    "roles/bigquery.jobUser",
    "roles/bigquery.dataViewer",
    "roles/bigquery.dataEditor",
    "roles/logging.logWriter",
    "roles/storage.objectAdmin" 
  ])
  project = var.project_id
  role    = each.key
  member  = "serviceAccount:${google_service_account.app_sa.email}"
}

resource "google_storage_bucket_iam_member" "storage_admin" {
  bucket = google_storage_bucket.almacen.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.app_sa.email}"
}

# 6. PAUSA DE SEGURIDAD PARA IAM (30 seg)
resource "time_sleep" "wait_30_seconds_iam" {
  depends_on = [
    google_project_iam_member.perms,
    google_storage_bucket_iam_member.storage_admin
  ]
  create_duration = "30s"
}

# 7. CLOUD RUN
resource "google_cloud_run_v2_service" "app" {
  name     = var.service_name
  location = var.region
  ingress  = "INGRESS_TRAFFIC_ALL"
  
  deletion_protection = false

  template {
    timeout = "900s"
    max_instance_request_concurrency = 1
    service_account = google_service_account.app_sa.email
    containers {
      image = "${var.region}-docker.pkg.dev/${var.project_id}/${var.repository_name}/streamlit-app:latest"
      
      resources {
        limits = {
          cpu    = "2"
          memory = "8Gi"
        }
      }
      
      env {
        name  = "PROJECT_ID"
        value = var.project_id
      }
      env {
        name  = "BUCKET_NAME"
        value = var.bucket_name
      }
      env {
        name  = "BQ_TABLE_PATH"
        value = var.bq_source_table
      }
    }
  }

  depends_on = [
    null_resource.build_and_push_image,
    time_sleep.wait_30_seconds_iam
  ]
}

resource "google_cloud_run_v2_service_iam_member" "public" {
  name     = google_cloud_run_v2_service.app.name
  location = google_cloud_run_v2_service.app.location
  role     = "roles/run.invoker"
  member   = "allUsers"
}

output "app_url" {
  value = google_cloud_run_v2_service.app.uri
}