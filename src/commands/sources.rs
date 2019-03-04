use default_editor;

use crate::config;

use crate::commands::{ApplicationArguments, SourcesCommand, SourcesSubCommand, SourcesAddOptions, SourcesDeleteOptions, SourcesEditOptions, SourcesListOptions,  DestinationCommand};
use crate::definitions::{DataSource, DataDestination};
#[cfg(feature = "use_mysql")]
use crate::sources::mysql::MysqlSource;
#[cfg(feature = "use_postgres")]
use crate::sources::postgres::PostgresSource;
#[cfg(feature = "use_sqlite")]
use crate::destinations::sqlite::SqliteDestination;

pub fn sources_add(args: &ApplicationArguments, sources_command: &SourcesCommand, add_options: &SourcesAddOptions) {
    config::save_source_config(&add_options.name, &add_options.source);
}

pub fn sources_delete(args: &ApplicationArguments, sources_command: &SourcesCommand, delete_options: &SourcesDeleteOptions) {
    let filename = config::get_sources_config_directory().join(delete_options.name.clone());
    std::fs::remove_file(filename).unwrap();
}

pub fn sources_edit(args: &ApplicationArguments, sources_command: &SourcesCommand, edit_options: &SourcesEditOptions) {
    let filename = config::get_sources_config_directory().join(edit_options.name.clone());
    if filename.exists() {
        match default_editor::get() {
            Ok(editor) => {
                std::process::Command::new(editor)
                    .args(&[filename])
                    .status()
                    .expect("could not run your text editor");
            },
            Err(error) => {
                eprintln!("Error: Could not figure out text editor to use: {}", error);
            }
        };
    } else {
        eprintln!("Error: File {} does not exist", filename.to_str().unwrap());
        std::process::exit(1);
    }
}

pub fn sources_list(args: &ApplicationArguments, sources_command: &SourcesCommand, list_options: &SourcesListOptions) {
    let dirname = config::get_sources_config_directory();
    let mut entries = if std::path::Path::new(&dirname).exists() {
        std::fs::read_dir(dirname)
            .unwrap()
            .map(|entry| entry.unwrap())
            .filter(|entry| !entry.file_type().unwrap().is_dir())
            .map(|entry| entry.file_name().into_string().unwrap())
            .collect()
    } else {
            vec![]
    };
    entries.sort();

    for entry in entries {
        println!("{}", entry);
    }
}

pub fn sources (args: &ApplicationArguments, sources_command: &SourcesCommand) {
    match &sources_command.command {
        SourcesSubCommand::Add(add_options) => sources_add(&args, &sources_command, &add_options),
        SourcesSubCommand::Delete(delete_options) => sources_delete(&args, &sources_command, &delete_options),
        SourcesSubCommand::Edit(edit_options) => sources_edit(&args, &sources_command, &edit_options),
        SourcesSubCommand::List(list_options) => sources_list(&args, &sources_command, &list_options),
    };
}
