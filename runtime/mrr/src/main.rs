use std::sync::Arc;

use clap::{Parser, Subcommand};
use uuid::Uuid;

use agentos_mrr::{RandomIdGenerator, RuntimeDb, SystemClock, TaskManager, TaskManagerConfig};

#[derive(Debug, Parser)]
#[command(name = "inspector_cli")]
#[command(about = "Read-only inspector for the AgentOS MRR v0.1 runtime")]
struct Cli {
    #[arg(
        long,
        env = "AGENTOS_MRR_DATABASE_URL",
        default_value = "sqlite://agentos-mrr.sqlite3"
    )]
    database_url: String,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Show {
        #[command(subcommand)]
        target: ShowTarget,
    },
}

#[derive(Debug, Subcommand)]
enum ShowTarget {
    Task { task_id: Uuid },
    Events { task_id: Uuid },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let db = RuntimeDb::connect_existing(&cli.database_url).await?;
    let manager = TaskManager::new(
        db.clone_pool(),
        Arc::new(SystemClock),
        Arc::new(RandomIdGenerator),
        TaskManagerConfig::default(),
    );

    match cli.command {
        Command::Show { target } => match target {
            ShowTarget::Task { task_id } => {
                let task = manager.get_task(task_id).await?;
                println!("{}", serde_json::to_string_pretty(&task)?);
            }
            ShowTarget::Events { task_id } => {
                let events = manager.list_events_by_task(task_id).await?;
                println!("{}", serde_json::to_string_pretty(&events)?);
            }
        },
    }

    Ok(())
}
