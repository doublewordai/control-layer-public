
#[cfg(feature = "embedded-db")]
use postgresql_embedded::{PostgreSQL, Settings, V16};
#[cfg(feature = "embedded-db")]
use std::path::PathBuf;
#[cfg(feature = "embedded-db")]
use tracing::{debug, info};

#[cfg(feature = "embedded-db")]
pub struct EmbeddedDatabase {
    postgres: PostgreSQL,
    connection_string: String,
}

#[cfg(feature = "embedded-db")]
impl EmbeddedDatabase {
    pub async fn start(data_dir: Option<PathBuf>, persistent: bool) -> anyhow::Result<Self> {
        let data_dir = data_dir.unwrap_or_else(|| {
            if let Some(home) = std::env::home_dir() {
                home.join(".dwctl_data").join("postgres")
            } else {
                PathBuf::from("dwctl_data/postgres")
            }
        });

        if persistent {
            use tracing::debug;

            debug!("Starting embedded PostgreSQL with data directory: {}", data_dir.display());
        } else {
            debug!("Starting ephemeral embedded PostgreSQL");
        }

        let settings = Settings {
            version: V16.clone(),
            port: 0,
            username: "postgres".to_string(),
            password: "password".to_string(),
            temporary: !persistent,
            installation_dir: data_dir.join("installation"),
            data_dir: data_dir.join("data"),
            ..Default::default()
        };

        let mut postgres = PostgreSQL::new(settings);

        postgres
            .setup()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to setup embedded PostgreSQL: {}", e))?;

        postgres
            .start()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to start embedded PostgreSQL: {}", e))?;

        let actual_port = postgres.settings().port;

        let database_name = "control_layer";
        match postgres.create_database(database_name).await {
            Ok(_) => {
                debug!("Created new database '{}'", database_name);
            }
            Err(e) => {
                let error_msg = e.to_string();
                if error_msg.contains("already exists") {
                    debug!("Database '{}' already exists, using existing database", database_name);
                } else {
                    return Err(anyhow::anyhow!("Failed to create database '{}': {}", database_name, e));
                }
            }
        }

        let connection_string = postgres.settings().url(database_name);

        info!("Embedded PostgreSQL started successfully on port {}", actual_port);

        Ok(Self {
            postgres,
            connection_string,
        })
    }

    pub fn connection_string(&self) -> &str {
        &self.connection_string
    }

    pub async fn stop(self) -> anyhow::Result<()> {
        info!("Stopping embedded PostgreSQL...");
        self.postgres
            .stop()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to stop embedded PostgreSQL: {}", e))?;
        info!("Embedded PostgreSQL stopped");
        Ok(())
    }
}

#[cfg(not(feature = "embedded-db"))]
pub struct EmbeddedDatabase;

#[cfg(not(feature = "embedded-db"))]
#[allow(dead_code)]
impl EmbeddedDatabase {
    pub async fn start(_data_dir: Option<std::path::PathBuf>, _persistent: bool) -> anyhow::Result<Self> {
        anyhow::bail!(
            "Embedded database feature is not enabled. \
             Rebuild with --features embedded-db to use this feature."
        )
    }

    pub fn connection_string(&self) -> &str {
        ""
    }

    pub fn port(&self) -> u16 {
        0
    }

    pub async fn stop(self) -> anyhow::Result<()> {
        Ok(())
    }
}
