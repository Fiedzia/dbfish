use std;
use std::io::Write;

use atty;
use termcolor;


use crate::commands::{ApplicationArguments, export::DebugDestinationOptions, UseColor};
use crate::definitions::{Row, DataSourceBatchIterator, DataDestination};
use crate::utils::fileorstdout::FileOrStdout;

pub struct DebugDestination {
    truncate: Option<u64>,
    column_names: Vec<String>,
    writer: FileOrStdout,
    use_color: bool,
}

impl DebugDestination {

    pub fn init(args: &ApplicationArguments, options: &DebugDestinationOptions) -> DebugDestination {
        let use_color = match args.color {
            UseColor::Yes => true,
            UseColor::No => false,
            UseColor::Auto => options.filename == "-" && atty::is(atty::Stream::Stdout),
        };

        DebugDestination {
            truncate: options.truncate,
            column_names: vec![],
            use_color,
            writer: match options.filename.as_ref() {
                "-" =>  FileOrStdout::ColorStdout(termcolor::StandardStream::stdout(termcolor::ColorChoice::Auto)),
                _ => FileOrStdout::File(std::fs::File::create(options.filename.clone()).unwrap())
            },
        }
    }
}

impl DataDestination for DebugDestination {
    
    fn prepare(&mut self) {}

    fn prepare_for_results(&mut self, result_iterator: &dyn DataSourceBatchIterator) {
        self.writer.write_all("#prepare_for_results\n".as_bytes()).unwrap();
        self.column_names = result_iterator
            .get_column_info()
            .iter()
            .map(|col| { col.name.clone() })
            .collect();
        self.writer.write_all("#columns\n".as_bytes()).unwrap();
        result_iterator
            .get_column_info()
            .iter()
            .for_each(|column| self.writer.write_all(format!("{:?}", column).as_bytes()).unwrap());
        self.writer.write(&['\n' as u8]);
    }

    fn add_rows(&mut self, rows: &[Row]) {

        for row in rows {
            for col in row.iter() {
                self.writer.write_all(format!("{:?}", col).as_bytes()).unwrap();
            }
            self.writer.write(&['\n' as u8]);
        }
    }

    fn close(&mut self) {
        self.writer.flush().unwrap();
    }


}

