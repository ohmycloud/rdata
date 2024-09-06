use arrow::array::AsArray;
use arrow::datatypes::{DataType, Field, Schema, UInt32Type};
use arrow::json::ReaderBuilder;
use serde::Serialize;
use std::sync::Arc;

#[derive(Serialize)]
struct MyStruct {
    language: String,
    age: u32,
}

fn main() -> anyhow::Result<()> {
    // define the schema
    let schema = Schema::new(vec![
        Field::new("language", DataType::Utf8, false),
        Field::new("age", DataType::UInt32, false),
    ]);

    let rows = vec![
        MyStruct {
            age: 18,
            language: "Rakudo".to_string(),
        },
        MyStruct {
            age: 15,
            language: "Rust".to_string(),
        },
    ];

    let mut decoder = ReaderBuilder::new(Arc::new(schema)).build_decoder()?;
    decoder.serialize(&rows)?;

    if let Some(batch) = decoder.flush()? {
        // Expect batch containing two columns
        let int32 = batch.column(1).as_primitive::<UInt32Type>();
        assert_eq!(int32.values(), &[18, 15]);

        let string = batch.column(0).as_string::<i32>();
        assert_eq!(string.value(0), "Rakudo");
        assert_eq!(string.value(1), "Rust");
    }

    Ok(())
}
