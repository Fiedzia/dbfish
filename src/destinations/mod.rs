use crate::definitions::{DataDestination, DataSourceBatchIterator, Row};

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
    #[cfg(feature = "use_csv")]
    CSV(csv::CSVDestination),
    #[cfg(feature = "use_html")]
    HTML(html::HTMLDestination),
    #[cfg(feature = "use_json")]
    JSON(json::JSONDestination),
    #[cfg(feature = "use_sqlite")]
    Sqlite(sqlite::SqliteDestination),
    #[cfg(feature = "use_spsheet")]
    SpreadSheet(ods_xlsx::SpreadSheetDestination),
    #[cfg(feature = "use_text")]
    Text(text::TextDestination),
    #[cfg(feature = "use_text")]
    TextVertical(text_vertical::TextVerticalDestination),
}

impl DataDestination for Destination {

    fn prepare(&mut self) {
        match self {
            #[cfg(feature = "use_csv")]
            Destination::CSV(csv_destination) => csv_destination.prepare(),
            #[cfg(feature = "use_html")]
            Destination::HTML(html_destination) => html_destination.prepare(),
            #[cfg(feature = "use_json")]
            Destination::JSON(json_destination) => json_destination.prepare(),
            #[cfg(feature = "use_spsheet")]
            Destination::SpreadSheet(spreadsheet_destination) => spreadsheet_destination.prepare(),
            #[cfg(feature = "use_sqlite")]
            Destination::Sqlite(sqlite_destination) => sqlite_destination.prepare(),
            #[cfg(feature = "use_text")]
            Destination::Text(text_destination) => text_destination.prepare(),
            #[cfg(feature = "use_text")]
            Destination::TextVertical(text_vertical_destination) => text_vertical_destination.prepare(),


        }
    }

    fn prepare_for_results(&mut self, result_iterator: &DataSourceBatchIterator) {
        match self {
            #[cfg(feature = "use_csv")]
            Destination::CSV(csv_destination) => csv_destination.prepare_for_results(result_iterator),
            #[cfg(feature = "use_html")]
            Destination::HTML(html_destination) => html_destination.prepare_for_results(result_iterator),
            #[cfg(feature = "use_json")]
            Destination::JSON(json_destination) => json_destination.prepare_for_results(result_iterator),
            #[cfg(feature = "use_spsheet")]
            Destination::SpreadSheet(spreadsheet_destination) => spreadsheet_destination.prepare_for_results(result_iterator),
            #[cfg(feature = "use_sqlite")]
            Destination::Sqlite(sqlite_destination) => sqlite_destination.prepare_for_results(result_iterator),
            #[cfg(feature = "use_text")]
            Destination::Text(text_destination) => text_destination.prepare_for_results(result_iterator),
            #[cfg(feature = "use_text")]
            Destination::TextVertical(text_vertical_destination) => text_vertical_destination.prepare_for_results(result_iterator),
                  }
    }

    fn add_rows(&mut self, rows: &[Row]) {
        match self {
            #[cfg(feature = "use_csv")]
            Destination::CSV(csv_destination) => csv_destination.add_rows(rows),
            #[cfg(feature = "use_html")]
            Destination::HTML(html_destination) => html_destination.add_rows(rows),
            #[cfg(feature = "use_json")]
            Destination::JSON(json_destination) => json_destination.add_rows(rows),
            #[cfg(feature = "use_spsheet")]
            Destination::SpreadSheet(spreadsheet_destination) => spreadsheet_destination.add_rows(rows),
            #[cfg(feature = "use_sqlite")]
            Destination::Sqlite(sqlite_destination) => sqlite_destination.add_rows(rows),
            #[cfg(feature = "use_text")]
            Destination::Text(text_destination) => text_destination.add_rows(rows),
            #[cfg(feature = "use_text")]
            Destination::TextVertical(text_vertical_destination) => text_vertical_destination.add_rows(rows),
        }
    }

    fn close(&mut self) {
        match self {
            #[cfg(feature = "use_csv")]
            Destination::CSV(csv_destination) => csv_destination.close(),
            #[cfg(feature = "use_html")]
            Destination::HTML(html_destination) => html_destination.close(),
            #[cfg(feature = "use_json")]
            Destination::JSON(json_destination) => json_destination.close(),
            #[cfg(feature = "use_spsheet")]
            Destination::SpreadSheet(spreadsheet_destination) => spreadsheet_destination.close(),
            #[cfg(feature = "use_sqlite")]
            Destination::Sqlite(sqlite_destination) => sqlite_destination.close(),
            #[cfg(feature = "use_text")]
            Destination::Text(text_destination) => text_destination.close(),
            #[cfg(feature = "use_text")]
            Destination::TextVertical(text_vertical_destination) => text_vertical_destination.close(),
        }
    }
}
