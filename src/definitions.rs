use chrono;

#[derive(Clone, Debug)]
pub enum ColumnType {
    U64,
    I64,
    U32,
    I32,
    U16,
    I16,
    U8,
    I8,
    F64,
    F32,
    String,
    Bytes,
    None, //mysql indicates that the column only stores null values. Not sure about sqlite.
    Date,
    DateTime,
    Time,
    Timestamp,
    Bool,
    Decimal,
    JSON,
    Custom(String), //things database-specific, or outside of this list. We can't map all types
}

#[derive(Debug)]
pub enum Value {
    U64(u64),
    I64(i64),
    U32(u32),
    I32(i32),
    U16(u16),
    I16(i16),
    U8(u8),
    I8(i8),
    F64(f64),
    F32(f32),
    String(String),
    Bytes(Vec<u8>),
    Bool(bool),
    JSON(String),
    None, //null
    Timestamp(u64),
    Date(chrono::NaiveDate),//year month day
    Time(chrono::NaiveTime),//hours, minutes, seconds
    DateTime(chrono::NaiveDateTime),//year month day, hours, minutes, seconds
    //Decimal(bigdecimal? decimal? string? what about precision?)
    //datetime with timezone
    Custom(String) //for all other existing types
}

pub type Row = Vec<Value>;

#[derive(Clone, Debug)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: ColumnType,
}


pub trait DataDestination
{
    fn prepare(&mut self);
    fn prepare_for_results(&mut self, result_iterator: &dyn DataSourceBatchIterator);
    fn add_rows(&mut self, rows: &[Row]);
    fn close(&mut self);
}


pub trait DataSourceBatchIterator<'conn> {
    fn get_column_info(&self) -> Vec<ColumnInfo>;
    fn get_count(&self) -> Option<u64>;
    fn next(&mut self) -> Option<Vec<Row>>;
}


pub trait DataSourceConnection<'conn>
{
    fn batch_iterator(&'conn mut self, batch_size: u64) -> Box<(dyn DataSourceBatchIterator<'conn> + 'conn)>;
}


pub trait DataSource<'source, 'conn, C>
where 
    C: DataSourceConnection<'conn> + 'source + 'conn,
    'source: 'conn
{
    fn connect(&'source self) -> C;
    fn get_type_name(&self) -> String;
    fn get_name(&self) -> String;
}
