#[cfg(feature = "use_csv")]
pub mod csv;
#[cfg(feature = "use_html")]
pub mod html;
#[cfg(feature = "use_json")]
pub mod json;
#[cfg(feature = "use_spsheet")]
pub mod ods_xlsx;
#[cfg(feature = "use_sqlite")]
pub mod sqlite;
#[cfg(feature = "use_text")]
pub mod text;
#[cfg(feature = "use_text")]
pub mod text_vertical;


pub enum Destination {

    #[cfg(feature = "use_sqlite")]
    Sqlite(sqlite::SqliteDestination)
}
