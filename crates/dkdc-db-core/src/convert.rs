use std::sync::Arc;

use arrow::array::{ArrayRef, BinaryBuilder, Float64Builder, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;

use crate::error::Result;

/// Convert turso rows into an Arrow RecordBatch.
pub async fn rows_to_record_batch(
    rows: &mut turso::Rows,
    schema: SchemaRef,
) -> Result<RecordBatch> {
    let num_cols = schema.fields().len();
    let mut builders: Vec<ColumnBuilder> = schema
        .fields()
        .iter()
        .map(|f| ColumnBuilder::new(f.data_type()))
        .collect();

    while let Some(row) = rows.next().await? {
        for (i, builder) in builders.iter_mut().enumerate() {
            builder.append_value(&row, i)?;
        }
    }

    let arrays: Vec<ArrayRef> = builders.into_iter().map(|b| b.finish()).collect();

    if arrays.is_empty() || (num_cols > 0 && arrays[0].is_empty()) {
        return Ok(RecordBatch::new_empty(schema));
    }

    Ok(RecordBatch::try_new(schema, arrays)?)
}

/// Convert turso rows into an Arrow RecordBatch, including an already-consumed first row.
pub async fn rows_to_record_batch_with_first(
    first_row: &turso::Row,
    rows: &mut turso::Rows,
    schema: SchemaRef,
) -> Result<RecordBatch> {
    let num_cols = schema.fields().len();
    let mut builders: Vec<ColumnBuilder> = schema
        .fields()
        .iter()
        .map(|f| ColumnBuilder::new(f.data_type()))
        .collect();

    // Append the first row
    for (i, builder) in builders.iter_mut().enumerate() {
        builder.append_value(first_row, i)?;
    }

    // Append remaining rows
    while let Some(row) = rows.next().await? {
        for (i, builder) in builders.iter_mut().enumerate() {
            builder.append_value(&row, i)?;
        }
    }

    let arrays: Vec<ArrayRef> = builders.into_iter().map(|b| b.finish()).collect();

    if arrays.is_empty() || (num_cols > 0 && arrays[0].is_empty()) {
        return Ok(RecordBatch::new_empty(schema));
    }

    Ok(RecordBatch::try_new(schema, arrays)?)
}

enum ColumnBuilder {
    Int64(Int64Builder),
    Float64(Float64Builder),
    Utf8(StringBuilder),
    Binary(BinaryBuilder),
}

impl ColumnBuilder {
    fn new(data_type: &DataType) -> Self {
        match data_type {
            DataType::Int64 => Self::Int64(Int64Builder::new()),
            DataType::Float64 => Self::Float64(Float64Builder::new()),
            DataType::Binary => Self::Binary(BinaryBuilder::new()),
            _ => Self::Utf8(StringBuilder::new()),
        }
    }

    fn append_value(&mut self, row: &turso::Row, idx: usize) -> Result<()> {
        let value = row.get_value(idx)?;
        match (self, value) {
            (Self::Int64(b), turso::Value::Integer(v)) => b.append_value(v),
            (Self::Int64(b), turso::Value::Null) => b.append_null(),
            (Self::Int64(b), turso::Value::Real(v)) => b.append_value(v as i64),
            (Self::Int64(b), turso::Value::Text(v)) => match v.parse::<i64>() {
                Ok(n) => b.append_value(n),
                Err(_) => b.append_null(),
            },

            (Self::Float64(b), turso::Value::Real(v)) => b.append_value(v),
            (Self::Float64(b), turso::Value::Null) => b.append_null(),
            (Self::Float64(b), turso::Value::Integer(v)) => b.append_value(v as f64),
            (Self::Float64(b), turso::Value::Text(v)) => match v.parse::<f64>() {
                Ok(n) => b.append_value(n),
                Err(_) => b.append_null(),
            },

            (Self::Utf8(b), turso::Value::Text(v)) => b.append_value(&v),
            (Self::Utf8(b), turso::Value::Null) => b.append_null(),
            (Self::Utf8(b), turso::Value::Integer(v)) => b.append_value(v.to_string()),
            (Self::Utf8(b), turso::Value::Real(v)) => b.append_value(v.to_string()),

            (Self::Binary(b), turso::Value::Blob(v)) => b.append_value(&v),
            (Self::Binary(b), turso::Value::Null) => b.append_null(),
            (Self::Binary(b), turso::Value::Text(v)) => b.append_value(v.as_bytes()),

            // Fallback: null for any unexpected combination
            (Self::Int64(b), _) => b.append_null(),
            (Self::Float64(b), _) => b.append_null(),
            (Self::Utf8(b), _) => b.append_null(),
            (Self::Binary(b), _) => b.append_null(),
        }
        Ok(())
    }

    fn finish(self) -> ArrayRef {
        match self {
            Self::Int64(mut b) => Arc::new(b.finish()),
            Self::Float64(mut b) => Arc::new(b.finish()),
            Self::Utf8(mut b) => Arc::new(b.finish()),
            Self::Binary(mut b) => Arc::new(b.finish()),
        }
    }
}
