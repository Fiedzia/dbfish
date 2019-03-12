use default_editor;

use crate::config;

use crate::commands::ApplicationArguments;
use crate::commands::common::SourceConfigCommand;

pub fn sources_add(_args: &ApplicationArguments, _sources_command: &SourcesCommand, add_options: &SourcesAddOptions) {
    config::save_source_config(&add_options.name, &add_options.source);
}

pub fn sources_delete(_args: &ApplicationArguments, _sources_command: &SourcesCommand, delete_options: &SourcesDeleteOptions) {
    let filename = config::get_sources_config_directory().join(delete_options.name.clone());
    std::fs::remove_file(filename).unwrap();
}

pub fn sources_edit(_args: &ApplicationArguments, _sources_command: &SourcesCommand, edit_options: &SourcesEditOptions) {
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

pub fn sources_list(_args: &ApplicationArguments, _sources_command: &SourcesCommand, _list_options: &SourcesListOptions) {

    let sources = config::get_sources_list();

    for source in sources {
        println!("{}", source.0);
    }
}

pub fn sources(args: &ApplicationArguments, sources_command: &SourcesCommand) {
    match &sources_command.command {
        SourcesSubCommand::Add(add_options) => sources_add(&args, &sources_command, &add_options),
        SourcesSubCommand::Delete(delete_options) => sources_delete(&args, &sources_command, &delete_options),
        SourcesSubCommand::Edit(edit_options) => sources_edit(&args, &sources_command, &edit_options),
        SourcesSubCommand::List(list_options) => sources_list(&args, &sources_command, &list_options),
    };
}

#[derive(Clone, StructOpt)]
pub struct SourcesCommand {
    #[structopt(subcommand)]
    pub command: SourcesSubCommand,
}


#[derive(Clone, Debug, StructOpt)]
pub enum SourcesSubCommand {
    #[structopt(name = "add", about="add source")]
    #[structopt(raw(setting = "structopt::clap::AppSettings::ColoredHelp"))]
    Add(SourcesAddOptions),
    #[structopt(name = "delete", about="delete source")]
    #[structopt(raw(setting = "structopt::clap::AppSettings::ColoredHelp"))]
    Delete(SourcesDeleteOptions),
    #[structopt(name = "edit", about="edit source definition")]
    #[structopt(raw(setting = "structopt::clap::AppSettings::ColoredHelp"))]
    Edit(SourcesEditOptions),
    #[structopt(name = "list", about="list sources")]
    #[structopt(raw(setting = "structopt::clap::AppSettings::ColoredHelp"))]
    List(SourcesListOptions),
}

#[derive(Clone, Debug, StructOpt)]
pub struct SourcesAddOptions {
    #[structopt(help = "source name")]
    pub name: String,
    #[structopt(subcommand)]
    pub source: SourceConfigCommand,
}

#[derive(Clone, Debug, StructOpt)]
pub struct SourcesDeleteOptions {
    #[structopt(help = "source name")]
    pub name: String,
}

#[derive(Clone, Debug, StructOpt)]
pub struct SourcesEditOptions {
    #[structopt(help = "source name")]
    pub name: String,
}

#[derive(Clone, Debug, StructOpt)]
pub struct SourcesListOptions {
}