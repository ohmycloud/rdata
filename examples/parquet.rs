use anyhow::anyhow;
use arrow::array::AsArray as ArrowAsArray;
use datafusion::arrow::array::AsArray;
use datafusion::prelude::SessionContext;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;
use polars::prelude::LazyFrame;
use polars::sql::SQLContext;

const INPUT_PATH: &str = "assets/users.parquet";

fn read_parquet(input_path: &str) -> anyhow::Result<()> {
    let file = File::open(input_path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?
        .with_batch_size(8192)
        .with_limit(10)
        .build()?;

    for record_batch in reader {
        let record_batch = record_batch?;
        if let Some(emails) = record_batch.column_by_name("email") {
            let emails = emails.as_binary::<i32>();
            for email in emails {
                if let Some(email) = email {
                    println!("{:?}", String::from_utf8_lossy(email));
                }
            }
        }
    }

    Ok(())
}

async fn read_with_datafusion(input_path: &str) -> anyhow::Result<()> {
    let ctx = SessionContext::new();
    ctx.register_parquet("users", input_path, Default::default())
        .await?;

    let ret = ctx
        .sql("SELECT email::text email, name::text name FROM users limit 10")
        .await?
        .collect()
        .await?;

    for batch_record in ret {
        let emails = batch_record
            .column_by_name("email")
            .ok_or(anyhow!("can't find email"))?
            .as_string::<i32>();

        let names = batch_record
            .column_by_name("name")
            .ok_or(anyhow!("can't find name"))?
            .as_string::<i32>();

        for (email, name) in emails.iter().zip(names.iter()) {
            let email = email.ok_or(anyhow!("can't find email"))?;
            let name = name.ok_or(anyhow!("can't find name"))?;
            println!("email: {:?}, names: {:?}",email,name);
        }
    }

    Ok(())
}

fn read_with_polars(input_path: &str) -> anyhow::Result<()> {
    let lf = LazyFrame::scan_parquet(input_path, Default::default())?;
    let mut ctx = SQLContext::new();
    ctx.register("users", lf);

    let df = ctx
        .execute("SELECT email::text email, name::text FROM users limit 10")?
        .collect()?;
    println!("{:?}", df);
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    read_parquet(INPUT_PATH)?;
    read_with_datafusion(INPUT_PATH).await?;
    read_with_polars(INPUT_PATH)?;
    Ok(())
}
