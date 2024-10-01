// use datafusion::functions_aggregate::expr_fn::min;
// use datafusion::physical_plan::analyze;

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::logical_expr::{Filter, LogicalPlan, LogicalTableSource, TableScan};
use datafusion::{error::DataFusionError, prelude::*};
use datafusion::logical_expr::{LogicalPlanBuilder};
use std::sync::Arc; // -> thread safe pointer

#[tokio::main]
async fn main() {
    // let _ = basic().await;
    // let _ = own_logical_plan();
    let _ = logicalplan_builder_tests();
}

#[allow(dead_code)]
async fn basic() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    ctx.register_csv("example", "data/example.csv", CsvReadOptions::new())
        .await?;

    // create a plan to run a SQL query
    let df = ctx
        .sql("EXPLAIN SELECT a FROM example WHERE a>2 and b<=4")
        .await?;

    // execute and print results
    df.show().await?;
    Ok(())
}

#[allow(dead_code)]
async fn dataframe_test() -> datafusion::error::Result<()> {
    // create the dataframe
    let ctx = SessionContext::new();
    let df = ctx
        .read_csv("data/example.csv", CsvReadOptions::new())
        .await?;

    let df = df
        .filter(col("a").gt(lit(2)).and(col("b").lt_eq(lit(4))))?
        .select(vec![col("a")])?
        .explain(false, false)?;

    df.show().await?;

    Ok(())
}

#[allow(dead_code)]
fn own_logical_plan() -> Result<(), DataFusionError> {
    // create a logical table source
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
    ]);
    let table_source = LogicalTableSource::new(SchemaRef::new(schema));

    // create a TableScan plan
    let projection = Some(vec![1]); // optional projection --> select name
    let filters = vec![col("id").gt(lit(400))]; // filter directly applied to table scan
    let fetch = None; // optional LIMIT
    let table_scan = LogicalPlan::TableScan(TableScan::try_new(
        "person",
        Arc::new(table_source),
        projection,
        filters,
        fetch,
    )?);

    // create a Filter plan that evaluates `id > 500` that wraps the TableScan
    let filter_expr = col("id").gt(lit(500));
    let plan = LogicalPlan::Filter(Filter::try_new(filter_expr, Arc::new(table_scan))?);

    // print the plan
    println!("{}", plan.display_indent_schema());
    Ok(())
}


#[allow(dead_code)]
fn logicalplan_builder_tests() -> Result<(), DataFusionError>{
    // create a logical table source
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
    ]);
    let table_source = LogicalTableSource::new(SchemaRef::new(schema));


    let projection = Some(vec![0]);

    let builder = LogicalPlanBuilder::scan("person", Arc::new(table_source), projection)?;
    let plan = builder.filter(col("id").gt(lit(500)))?.build()?;

    println!("{}", plan.display_indent_schema());
    Ok(())
}
