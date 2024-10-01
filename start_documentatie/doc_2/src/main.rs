use std::any::Any;
use std::sync::Arc;
// use arrow::datatypes::DataType;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{plan_err, DataFusionError, Result};
use datafusion::datasource::MemTable;
use datafusion::logical_expr::{col, create_udf, ColumnarValue, Signature, Volatility};
use datafusion::logical_expr::{ScalarUDF, ScalarUDFImpl};

use datafusion::arrow::array::{ArrayRef, Int64Array, Int64Builder, RecordBatch};
use datafusion::common::cast::as_int64_array;
use datafusion::physical_plan::functions::columnar_values_to_array;

use datafusion::arrow::util::pretty;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::{create_udaf, create_udwf, CsvReadOptions, Expr};

use datafusion::arrow::{
    array::{AsArray, Float64Array},
    datatypes::Float64Type,
};
use datafusion::logical_expr::PartitionEvaluator;
use datafusion::scalar::ScalarValue;

use datafusion::physical_plan::Accumulator;

use datafusion::datasource::function::TableFunctionImpl;

// SCALAR UDF
fn add_one(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    // Error handling omitted for brevity
    let args = columnar_values_to_array(args)?;
    let i64s = as_int64_array(&args[0])?;

    let new_array = i64s
        .iter()
        .map(|array_elem| array_elem.map(|value| value + 1))
        .collect::<Int64Array>();

    Ok(ColumnarValue::Array(Arc::new(new_array)))
}

fn times_two(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let args = columnar_values_to_array(args)?;
    let i64s = as_int64_array(&args[0])?;

    let new_array = i64s
        .iter()
        .map(|array_elem| array_elem.map(|value| value * 2))
        .collect::<Int64Array>();

    Ok(ColumnarValue::Array(Arc::new(new_array)))
}

fn add_one_tester() {
    let input = vec![Some(1), None, Some(3)];
    let input = Arc::new(Int64Array::from(input)) as ArrayRef;

    let result = add_one(&[datafusion::logical_expr::ColumnarValue::Array(input)]).unwrap();

    println!("about to test");

    // Extract the Int64Array from the ColumnarValue
    if let datafusion::logical_expr::ColumnarValue::Array(array) = result {
        let result_array = array.as_any().downcast_ref::<Int64Array>().unwrap();

        // Now assert_eq! works since both are Int64Array
        assert_eq!(
            result_array,
            &Int64Array::from(vec![Some(2), None, Some(4)])
        );
    } else {
        panic!("Expected an Array result");
    }
}

//WINDOW UDF
#[derive(Clone, Debug)]
struct MyPartitionEvaluator {}

impl MyPartitionEvaluator {
    fn new() -> Self {
        Self {}
    }
}

impl PartitionEvaluator for MyPartitionEvaluator {
    fn uses_window_frame(&self) -> bool {
        true
    }

    fn evaluate(
        &mut self,
        _values: &[ArrayRef],
        _range: &std::ops::Range<usize>,
    ) -> Result<datafusion::scalar::ScalarValue> {
        //array extracten
        let arr: &Float64Array = _values[0].as_ref().as_primitive::<Float64Type>();

        //totale range berekenen voor de effectieve berekeningen
        let range_len = _range.end - _range.start;

        //effectieve berekening
        let output = if range_len > 0 {
            let sum: f64 = arr.values().iter().skip(_range.start).take(range_len).sum();
            Some(sum / range_len as f64)
        } else {
            None
        };
        Ok(ScalarValue::Float64(output))
    }
}

fn make_partition_evaluator() -> Result<Box<dyn PartitionEvaluator>> {
    Ok(Box::new(MyPartitionEvaluator::new()))
}

// AGGREGATE UDF

//struct omdat we state bij moeten houden
#[derive(Debug)]
struct GeometricMean {
    n: u32,
    prod: f64,
}

impl GeometricMean {
    //constructior
    pub fn new() -> Self {
        GeometricMean { n: 0, prod: 1.0 }
    }
}

//Accumulator wordt gebruikt om user defined aggregate functions te maken
impl Accumulator for GeometricMean {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::from(self.prod),
            ScalarValue::from(self.n),
        ])
    }

    //return final value van de aggregatie
    fn evaluate(&mut self) -> Result<ScalarValue> {
        let value = self.prod.powf(1.0 / self.n as f64);
        Ok(ScalarValue::from(value))
    }

    //update state van accumulator voor een batch input --> product updaten met values en count updaten met aantal values
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        let arr = &values[0];
        (0..arr.len()).try_for_each(|index| {
            let v = ScalarValue::try_from_array(arr, index)?;
            if let ScalarValue::Float64(Some(value)) = v {
                self.prod *= value;
                self.n += 1;
            } else {
                unreachable!("")
            }
            Ok(())
        })
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        let arr = &states[0];

        (0..arr.len()).try_for_each(|index| {
            let v = states
                .iter()
                .map(|array| ScalarValue::try_from_array(array, index))
                .collect::<Result<Vec<_>>>()?;
            if let (ScalarValue::Float64(Some(prod)), ScalarValue::UInt32(Some(n))) = (&v[0], &v[1])
            {
                self.prod *= prod;
                self.n += n;
            } else {
                unreachable!("")
            }
            Ok(())
        })
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}

//USER DEFINED TABLE FUNCTION
#[derive(Default)]
pub struct EchoFunction {}

impl TableFunctionImpl for EchoFunction {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn datafusion::catalog::TableProvider>> {
        let Some(Expr::Literal(ScalarValue::Int64(Some(value)))) = exprs.get(0) else {
            return plan_err!("First argument must be an integer");
        };

        //maak schema
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));

        // maak een recordbatch met de waarde als een kolom

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![*value + 1]))],
        )?;

        let provider = MemTable::try_new(schema, vec![vec![batch]])?;

        Ok(Arc::new(provider))
    }
}

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    println!("Hello, world!");

    add_one_tester();

    let udf = create_udf(
        "add_one",
        vec![DataType::Int64],
        Arc::new(DataType::Int64),
        Volatility::Immutable,
        Arc::new(add_one),
    );

    let mut ctx = SessionContext::new();

    ctx.register_csv("example", "data/data.csv", CsvReadOptions::new())
        .await?;

    ctx.register_udf(udf);

    let sql = "SELECT add_one(1)";

    let df = ctx.sql(&sql).await.unwrap();

    let results = df.collect().await.unwrap();

    pretty::print_batches(&results).unwrap();

    let udf_2 = create_udf(
        "times_two",
        vec![DataType::Int64],
        Arc::new(DataType::Int64),
        Volatility::Immutable,
        Arc::new(times_two),
    );

    ctx.register_udf(udf_2);

    let sql_2 = "SELECT times_two(a) as multiplied_value FROM example ";

    let df_2 = ctx.sql(&sql_2).await.unwrap();

    let results_2 = df_2.collect().await.unwrap();

    pretty::print_batches(&results_2).unwrap();

    let smooth_it = create_udwf(
        "smooth_it",
        DataType::Float64,
        Arc::new(DataType::Float64),
        Volatility::Immutable,
        Arc::new(make_partition_evaluator),
    );

    ctx.register_udwf(smooth_it);
    let csv_path = "data/cars.csv".to_string();
    ctx.register_csv(
        "cars",
        &csv_path,
        CsvReadOptions::default().has_header(true),
    )
    .await?;
    // do query with smooth_it
    let df_3 = ctx
        .sql(
            "SELECT \
               car, \
               speed, \
               smooth_it(speed) OVER (PARTITION BY car ORDER BY time) as smooth_speed,\
               time \
               from cars \
             ORDER BY \
               car",
        )
        .await?;

    let results_3 = df_3.collect().await.unwrap();

    pretty::print_batches(&results_3).unwrap();

    let csv_path = "data/data2.csv".to_string();
    ctx.register_csv(
        "example2",
        &csv_path,
        CsvReadOptions::default().has_header(true),
    )
    .await?;

    let df_4 = ctx
        .sql("SELECT c,e, smooth_it(e) OVER (PARTITION BY c ORDER BY a) as smooth, a FROM example2 ORDER BY b")
        .await?;

    let results_4 = df_4.collect().await.unwrap();

    pretty::print_batches(&results_4).unwrap();

    let geometric_mean = create_udaf(
        "geo_mean",
        vec![DataType::Float64],
        Arc::new(DataType::Float64),
        Volatility::Immutable,
        Arc::new(|_| Ok(Box::new(GeometricMean::new()))),
        Arc::new(vec![DataType::Float64, DataType::UInt32]),
    );

    ctx.register_udaf(geometric_mean);

    let sql_5 = "SELECT geo_mean(a) from example";

    let df_5 = ctx.sql(&sql_5).await?;

    let results_5 = df_5.collect().await.unwrap();

    pretty::print_batches(&results_5).unwrap();

    ctx.register_udtf("echo_p_one", Arc::new(EchoFunction::default()));

    let df_6 = ctx.sql("SELECT a from echo_p_one(example)").await?;

    let results_6 = df_6.collect().await?;

    pretty::print_batches(&results_6)?;

    Ok(())
}
