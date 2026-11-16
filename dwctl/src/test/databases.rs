
use sqlx::postgres::PgPoolOptions;
use sqlx::{Executor, PgPool};

pub struct TestDatabases {
    admin_pool: PgPool,
    fusillade_db_name: String,
    outlet_db_name: String,
    pub fusillade_url: String,
    pub outlet_url: String,
}

impl TestDatabases {
    pub async fn new(main_pool: &PgPool, test_prefix: &str) -> anyhow::Result<Self> {
        let safe_prefix: String = test_prefix.chars().map(|c| if c.is_alphanumeric() { c } else { '_' }).collect();

        let fusillade_db_name = format!("test_{}_fusillade", safe_prefix);
        let outlet_db_name = format!("test_{}_outlet", safe_prefix);

        let connect_opts = main_pool.connect_options();
        let opts = connect_opts.as_ref();

        let base_url = build_connection_url(opts, "postgres");
        let admin_pool = PgPoolOptions::new().max_connections(2).connect(&base_url).await?;

        Self::drop_database_if_exists(&admin_pool, &fusillade_db_name).await?;
        Self::drop_database_if_exists(&admin_pool, &outlet_db_name).await?;

        admin_pool
            .execute(format!("CREATE DATABASE {}", fusillade_db_name).as_str())
            .await?;
        admin_pool.execute(format!("CREATE DATABASE {}", outlet_db_name).as_str()).await?;

        let fusillade_url = build_connection_url(opts, &fusillade_db_name);
        let outlet_url = build_connection_url(opts, &outlet_db_name);

        Ok(Self {
            admin_pool,
            fusillade_db_name,
            outlet_db_name,
            fusillade_url,
            outlet_url,
        })
    }

    pub async fn cleanup(self) -> anyhow::Result<()> {
        Self::drop_database_if_exists(&self.admin_pool, &self.fusillade_db_name).await?;
        Self::drop_database_if_exists(&self.admin_pool, &self.outlet_db_name).await?;
        self.admin_pool.close().await;
        Ok(())
    }

    async fn drop_database_if_exists(admin_pool: &PgPool, db_name: &str) -> anyhow::Result<()> {
        admin_pool
            .execute(
                format!(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{}'",
                    db_name
                )
                .as_str(),
            )
            .await
            .ok();

        admin_pool.execute(format!("DROP DATABASE IF EXISTS {}", db_name).as_str()).await?;

        Ok(())
    }
}

fn build_connection_url(opts: &sqlx::postgres::PgConnectOptions, database: &str) -> String {
    let host = opts.get_host();
    let port = opts.get_port();
    let username = opts.get_username();

    if let Ok(base_url) = std::env::var("DATABASE_URL") {
        if let Ok(mut url) = url::Url::parse(&base_url) {
            url.set_path(&format!("/{}", database));
            return url.to_string();
        }
    }

    format!("postgres://{}@{}:{}/{}", username, host, port, database)
}
