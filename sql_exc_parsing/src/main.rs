//schrijf een programma van een ‘query interpreter’ die een data directory & queries als invoer aanneemt.
//(1) registreer alle invoerrelaties in de SessionContext
//(2) de queries uitvoert, MAAR voer de stappen (SQL -> logisch plan -> …) expliciet apart uit (vermijd de .sql() functie).

use datafusion::common::Result;

use datafusion::execution::TaskContext;

use datafusion::prelude::*;

use datafusion::arrow::util::pretty::pretty_format_batches;

use std::fs;
use std::sync::Arc;

use clap::Parser as ClapParser; //moet om naming conflicts te voorkomen
/// Simple program to greet a person
#[derive(ClapParser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value_t = String::from("SELECT * from data2 where a>2"))]
    query: String,

    #[arg(short, long, default_value_t = String::from("data/"))]
    data: String,
}

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let args = Args::parse();

    let sql = &args.query;
    let data = &args.data;

    println!("{} {}", sql, data);

    let ctx = SessionContext::new();

    let paths = fs::read_dir(data).unwrap();

    for path in paths {
        let s = path.unwrap().path().display().to_string();
        let last = {
            let split_pos = s.len() - 8;
            &s[split_pos..]
        };
        if last == ".parquet" {
            let f = extract_name(&s);
            println!("Adding {} from {}", f, s);
            ctx.register_parquet(f, &s, ParquetReadOptions::default())
                .await?;
        }
    }
    // ctx.register_parquet("dat", data, ParquetReadOptions::default()).await?;

    // normal_sql(ctx, sql).await?;
    parse_sql_self(ctx, sql).await?;

    Ok(())
}

fn extract_name(s: &str) -> &str {
    let split_pos = s.len() - 8;
    let start_pos = s.rfind('/');
    let start_pos = start_pos.unwrap();
    &s[start_pos + 1..split_pos]
}

#[warn(dead_code)]
async fn normal_sql(ctx: SessionContext, sql: &str) -> datafusion::error::Result<()> {
    let df = ctx.sql(sql).await?;

    // let results = df.collect().await?;

    df.show().await?;

    Ok(())
}

async fn parse_sql_self(ctx: SessionContext, query: &str) -> Result<()> {
    let log_plan = ctx.state().create_logical_plan(query).await?;

    let phys_plan = ctx.state().create_physical_plan(&log_plan).await?;

    let task_ctx = Arc::new(TaskContext::from(&ctx)); //task_ctx niet zelf gevonden

    let results = datafusion::physical_plan::collect(phys_plan.clone(), task_ctx.clone()).await?;

    let formatted = pretty_format_batches(&results)?.to_string();
    println!("{}", formatted);

    Ok(())
}
