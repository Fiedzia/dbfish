use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::temporal_conversions::time_to_time64ns;
use arrow::array::types::Date32Type;
use arrow::array::{
    BinaryBuilder, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder, Int16Builder,
    Int32Builder, Int64Builder, Int8Builder, StringBuilder, StructArray, Time64NanosecondBuilder,
    TimestampSecondBuilder, UInt16Builder, UInt32Builder, UInt64Builder, UInt8Builder,
};
use arrow::datatypes::DataType;
use arrow::datatypes::{Field, Fields, Schema, SchemaBuilder, TimeUnit};
use parquet::arrow::ArrowWriter as ParquetWriter;
//use parquet::basic::Encoding;
//use parquet::errors::Result;
use parquet::file::properties::WriterProperties;

use crate::commands::export::ParquetDestinationOptions;
use crate::definitions::{ColumnType, DataDestination, DataSourceBatchIterator, Row, Value};
use crate::utils::truncate_text_with_note;

pub struct ParquetDestination {
    //column_names: Vec<String>,
    truncate: Option<u64>,
    filename: String,
    //file: File,
    //schema_builder: SchemaBuilder,
    //writer_properties: Option<WriterProperties>,
    writer: Option<ParquetWriter<File>>,
    schema: Option<Arc<Schema>>,
}

impl ParquetDestination {
    pub fn init(parquet_options: &ParquetDestinationOptions) -> ParquetDestination {
        let path = Path::new(&parquet_options.filename);
        if path.exists() {
            std::fs::remove_file(path).unwrap();
        }
        ParquetDestination {
            filename: parquet_options.filename.clone(),
            //file: File::create(&parquet_options.filename).unwrap(),
            //column_names: vec![],
            truncate: parquet_options.truncate,
            //schema: Schema::new(vec![Field::new("id", UInt64, false)]),
            //schema: Schema::new(Vec::<Field>::new()),
            //schema_builder: SchemaBuilder::new(),
            schema: None,
            //writer_properties: None,
            writer: None,
        }
    }
}

impl DataDestination for ParquetDestination {
    fn prepare(&mut self) {}

    fn prepare_for_results(&mut self, result_iterator: &dyn DataSourceBatchIterator) {
        let mut schema_builder = SchemaBuilder::new();
        for col in result_iterator.get_column_info().iter() {
            match col.data_type {
                ColumnType::U64 => {
                    schema_builder.push(Field::new(col.name.clone(), DataType::UInt64, true))
                }
                ColumnType::I64 => {
                    schema_builder.push(Field::new(col.name.clone(), DataType::Int64, true))
                }
                ColumnType::U32 => {
                    schema_builder.push(Field::new(col.name.clone(), DataType::UInt32, true))
                }
                ColumnType::I32 => {
                    schema_builder.push(Field::new(col.name.clone(), DataType::Int32, true))
                }
                ColumnType::U16 => {
                    schema_builder.push(Field::new(col.name.clone(), DataType::UInt16, true))
                }
                ColumnType::I16 => {
                    schema_builder.push(Field::new(col.name.clone(), DataType::Int16, true))
                }
                ColumnType::U8 => {
                    schema_builder.push(Field::new(col.name.clone(), DataType::UInt8, true))
                }
                ColumnType::I8 => {
                    schema_builder.push(Field::new(col.name.clone(), DataType::Int8, true))
                }
                //Utf8 is 32bit, LargeUtf8 64bit
                ColumnType::String => {
                    schema_builder.push(Field::new(col.name.clone(), DataType::Utf8, true))
                }
                ColumnType::Bytes => {
                    schema_builder.push(Field::new(col.name.clone(), DataType::Binary, true))
                }
                ColumnType::F64 => {
                    schema_builder.push(Field::new(col.name.clone(), DataType::Float64, true))
                }
                ColumnType::F32 => {
                    schema_builder.push(Field::new(col.name.clone(), DataType::Float32, true))
                }
                ColumnType::Bool => {
                    schema_builder.push(Field::new(col.name.clone(), DataType::Boolean, true))
                }

                ColumnType::DateTime => schema_builder.push(Field::new(
                    col.name.clone(),
                    DataType::Timestamp(TimeUnit::Second, None),
                    true,
                )),
                ColumnType::Date => {
                    schema_builder.push(Field::new(col.name.clone(), DataType::Date32, true))
                }
                ColumnType::Time => schema_builder.push(Field::new(
                    col.name.clone(),
                    DataType::Time32(TimeUnit::Second),
                    true,
                )),
                //ColumnType::Decimal => self.schema.fields.push(DataType::Decimal123(u8,i8)),
                _ => panic!("parquet: unsupported column type: {:?}", col.data_type),
            }
        }

        let writer_properties_builder = WriterProperties::builder();
        let writer_properties = writer_properties_builder.build();

        let schema = Arc::new(schema_builder.finish());
        self.schema = Some(schema.clone());
        let file = File::create(&self.filename).unwrap();
        let writer = ParquetWriter::try_new(file, schema, Some(writer_properties)).unwrap();

        self.writer = Some(writer);
    }

    fn add_rows(&mut self, rows: &[Row]) {
        for (col_idx, field) in (&self.schema.as_ref().unwrap().fields)
            .into_iter()
            .enumerate()
        {
            match field.data_type() {
                DataType::UInt64 => {
                    let mut array = UInt64Builder::new();
                    rows.iter().for_each(|row| {
                        if let Value::U64(v) = row[col_idx] {
                            array.append_value(v);
                        } else {
                            array.append_null();
                        }
                    });
                    self.writer
                        .as_mut()
                        .unwrap()
                        .write(
                            &StructArray::new(
                                Fields::from(vec![field.clone()]),
                                vec![Arc::new(array.finish())],
                                None,
                            )
                            .into(),
                        )
                        .unwrap();
                }
                DataType::Int64 => {
                    let mut array = Int64Builder::new();
                    rows.iter().for_each(|row| {
                        if let Value::I64(v) = row[col_idx] {
                            array.append_value(v);
                        } else {
                            array.append_null();
                        }
                    });
                    self.writer
                        .as_mut()
                        .unwrap()
                        .write(
                            &StructArray::new(
                                Fields::from(vec![field.clone()]),
                                vec![Arc::new(array.finish())],
                                None,
                            )
                            .into(),
                        )
                        .unwrap();
                }

                DataType::UInt32 => {
                    let mut array = UInt32Builder::new();
                    rows.iter().for_each(|row| {
                        if let Value::U32(v) = row[col_idx] {
                            array.append_value(v);
                        } else {
                            array.append_null();
                        }
                    });
                    self.writer
                        .as_mut()
                        .unwrap()
                        .write(
                            &StructArray::new(
                                Fields::from(vec![field.clone()]),
                                vec![Arc::new(array.finish())],
                                None,
                            )
                            .into(),
                        )
                        .unwrap();
                }
                DataType::Int32 => {
                    let mut array = Int32Builder::new();
                    rows.iter().for_each(|row| {
                        if let Value::I32(v) = row[col_idx] {
                            array.append_value(v);
                        } else {
                            array.append_null();
                        }
                    });
                    self.writer
                        .as_mut()
                        .unwrap()
                        .write(
                            &StructArray::new(
                                Fields::from(vec![field.clone()]),
                                vec![Arc::new(array.finish())],
                                None,
                            )
                            .into(),
                        )
                        .unwrap();
                }
                DataType::UInt16 => {
                    let mut array = UInt16Builder::new();
                    rows.iter().for_each(|row| {
                        if let Value::U16(v) = row[col_idx] {
                            array.append_value(v);
                        } else {
                            array.append_null();
                        }
                    });
                    self.writer
                        .as_mut()
                        .unwrap()
                        .write(
                            &StructArray::new(
                                Fields::from(vec![field.clone()]),
                                vec![Arc::new(array.finish())],
                                None,
                            )
                            .into(),
                        )
                        .unwrap();
                }
                DataType::Int16 => {
                    let mut array = Int16Builder::new();
                    rows.iter().for_each(|row| {
                        if let Value::I16(v) = row[col_idx] {
                            array.append_value(v);
                        } else {
                            array.append_null();
                        }
                    });
                    self.writer
                        .as_mut()
                        .unwrap()
                        .write(
                            &StructArray::new(
                                Fields::from(vec![field.clone()]),
                                vec![Arc::new(array.finish())],
                                None,
                            )
                            .into(),
                        )
                        .unwrap();
                }
                DataType::UInt8 => {
                    let mut array = UInt8Builder::new();
                    rows.iter().for_each(|row| {
                        if let Value::U8(v) = row[col_idx] {
                            array.append_value(v);
                        } else {
                            array.append_null();
                        }
                    });
                    self.writer
                        .as_mut()
                        .unwrap()
                        .write(
                            &StructArray::new(
                                Fields::from(vec![field.clone()]),
                                vec![Arc::new(array.finish())],
                                None,
                            )
                            .into(),
                        )
                        .unwrap();
                }
                DataType::Int8 => {
                    let mut array = Int8Builder::new();
                    rows.iter().for_each(|row| {
                        if let Value::I8(v) = row[col_idx] {
                            array.append_value(v);
                        } else {
                            array.append_null();
                        }
                    });
                    self.writer
                        .as_mut()
                        .unwrap()
                        .write(
                            &StructArray::new(
                                Fields::from(vec![field.clone()]),
                                vec![Arc::new(array.finish())],
                                None,
                            )
                            .into(),
                        )
                        .unwrap();
                }
                DataType::Float64 => {
                    let mut array = Float64Builder::new();
                    rows.iter().for_each(|row| {
                        if let Value::F64(v) = row[col_idx] {
                            array.append_value(v);
                        } else {
                            array.append_null();
                        }
                    });
                    self.writer
                        .as_mut()
                        .unwrap()
                        .write(
                            &StructArray::new(
                                Fields::from(vec![field.clone()]),
                                vec![Arc::new(array.finish())],
                                None,
                            )
                            .into(),
                        )
                        .unwrap();
                }
                DataType::Float32 => {
                    let mut array = Float32Builder::new();
                    rows.iter().for_each(|row| {
                        if let Value::F32(v) = row[col_idx] {
                            array.append_value(v);
                        } else {
                            array.append_null();
                        }
                    });
                    self.writer
                        .as_mut()
                        .unwrap()
                        .write(
                            &StructArray::new(
                                Fields::from(vec![field.clone()]),
                                vec![Arc::new(array.finish())],
                                None,
                            )
                            .into(),
                        )
                        .unwrap();
                }
                DataType::Boolean => {
                    let mut array = BooleanBuilder::new();
                    rows.iter().for_each(|row| {
                        if let Value::Bool(v) = row[col_idx] {
                            array.append_value(v);
                        } else {
                            array.append_null();
                        }
                    });
                    self.writer
                        .as_mut()
                        .unwrap()
                        .write(
                            &StructArray::new(
                                Fields::from(vec![field.clone()]),
                                vec![Arc::new(array.finish())],
                                None,
                            )
                            .into(),
                        )
                        .unwrap();
                }
                DataType::Utf8 => {
                    let mut array = StringBuilder::new();
                    rows.iter().for_each(|row| {
                        if let Value::String(ref s) = row[col_idx] {
                            array.append_value(truncate_text_with_note(s.clone(), self.truncate));
                        } else {
                            array.append_null();
                        }
                    });
                    self.writer
                        .as_mut()
                        .unwrap()
                        .write(
                            &StructArray::new(
                                Fields::from(vec![field.clone()]),
                                vec![Arc::new(array.finish())],
                                None,
                            )
                            .into(),
                        )
                        .unwrap();
                }

                DataType::Timestamp(_unit, _optional_timezone) => {
                    //FIXME: handle unit and timezone. For now it's only seconds
                    let mut array = TimestampSecondBuilder::new();
                    rows.iter().for_each(|row| {
                        if let Value::Timestamp(t) = row[col_idx] {
                            array.append_value(t as i64);
                        } else {
                            array.append_null();
                        }
                    });
                    self.writer
                        .as_mut()
                        .unwrap()
                        .write(
                            &StructArray::new(
                                Fields::from(vec![field.clone()]),
                                vec![Arc::new(array.finish())],
                                None,
                            )
                            .into(),
                        )
                        .unwrap();
                }

                DataType::Date32 => {
                    let mut array = Date32Builder::new();
                    rows.iter().for_each(|row| {
                        if let Value::Date(ref d) = row[col_idx] {
                            array.append_value(Date32Type::from_naive_date(*d));
                        } else {
                            array.append_null();
                        }
                    });
                    self.writer
                        .as_mut()
                        .unwrap()
                        .write(
                            &StructArray::new(
                                Fields::from(vec![field.clone()]),
                                vec![Arc::new(array.finish())],
                                None,
                            )
                            .into(),
                        )
                        .unwrap();
                }

                DataType::Time32(_unit) => {
                    //FIXME: assuming unit is second for now,
                    let mut array = Time64NanosecondBuilder::new();
                    rows.iter().for_each(|row| {
                        if let Value::Time(t) = row[col_idx] {
                            array.append_value(time_to_time64ns(t));
                        } else {
                            array.append_null();
                        }
                    });
                    self.writer
                        .as_mut()
                        .unwrap()
                        .write(
                            &StructArray::new(
                                Fields::from(vec![field.clone()]),
                                vec![Arc::new(array.finish())],
                                None,
                            )
                            .into(),
                        )
                        .unwrap();
                }
                DataType::Binary => {
                    //FIXME: assuming unit is second for now,
                    let mut array = BinaryBuilder::new();
                    rows.iter().for_each(|row| {
                        if let Value::Bytes(ref b) = row[col_idx] {
                            array.append_value(b.clone());
                        } else {
                            array.append_null();
                        }
                    });
                    self.writer
                        .as_mut()
                        .unwrap()
                        .write(
                            &StructArray::new(
                                Fields::from(vec![field.clone()]),
                                vec![Arc::new(array.finish())],
                                None,
                            )
                            .into(),
                        )
                        .unwrap();
                }

                _ => panic!("Parquet: unsupported data type{}", field.data_type()),
            }
        }
    }

    fn close(&mut self) {
        self.writer.as_mut().unwrap().flush().unwrap();
        //self.writer.as_mut().unwrap().close().unwrap();
    }
}
