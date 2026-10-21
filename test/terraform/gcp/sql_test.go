package gcp_test

import "testing"

// The gopherstack container has no container runtime socket, so Cloud SQL runs state-only here: the
// resources round-trip through the Admin API without a real database engine.
const sqlFixture = `
resource "google_sql_database_instance" "pg" {
  name                = "gopherstack-tf-pg"
  database_version    = "POSTGRES_16"
  region              = "us-central1"
  root_password       = "tf-root-password"
  deletion_protection = false

  settings {
    tier = "db-custom-1-3840"

    database_flags {
      name  = "max_connections"
      value = "150"
    }
  }
}

resource "google_sql_database" "app" {
  name     = "app"
  instance = google_sql_database_instance.pg.name
}

resource "google_sql_user" "app" {
  name     = "app_user"
  instance = google_sql_database_instance.pg.name
  password = "tf-user-password"
}

resource "google_sql_database_instance" "my" {
  name                = "gopherstack-tf-my"
  database_version    = "MYSQL_8_0"
  region              = "us-central1"
  deletion_protection = false

  settings {
    tier = "db-f1-micro"
  }
}

resource "google_sql_database" "mydb" {
  name     = "shop"
  instance = google_sql_database_instance.my.name
}

resource "google_sql_user" "myuser" {
  name     = "shop_user"
  host     = "%"
  instance = google_sql_database_instance.my.name
  password = "tf-user-password"
}
`

func TestTerraformGCP_SQL(t *testing.T) {
	t.Parallel()

	newRunner(t, sqlFixture).lifecycle()
}
