use std::cmp::max;

use default_editor;
use regex::RegexBuilder;

use crate::config;

use crate::commands::common::SourceConfigCommand;
use crate::commands::ApplicationArguments;

pub fn sources_add(
    _args: &ApplicationArguments,
    _sources_command: &SourcesCommand,
    add_options: &SourcesAddOptions,
) {
    config::save_source_config(&add_options.name, &add_options.source);
}

pub fn sources_delete(
    _args: &ApplicationArguments,
    _sources_command: &SourcesCommand,
    delete_options: &SourcesDeleteOptions,
) {
    let filename = config::get_sources_config_directory().join(delete_options.name.clone());
    std::fs::remove_file(filename).unwrap();
}

pub fn sources_edit(
    _args: &ApplicationArguments,
    _sources_command: &SourcesCommand,
    edit_options: &SourcesEditOptions,
) {
    let filename = config::get_sources_config_directory().join(edit_options.name.clone());
    if filename.exists() {
        match default_editor::get() {
            Ok(editor) => {
                std::process::Command::new(editor)
                    .args(&[filename])
                    .status()
                    .expect("could not run your text editor");
            }
            Err(error) => {
                eprintln!("Error: Could not figure out text editor to use: {}", error);
            }
        };
    } else {
        eprintln!("Error: File {} does not exist", filename.to_str().unwrap());
        std::process::exit(1);
    }
}

pub fn sources_list(
    _args: &ApplicationArguments,
    _sources_command: &SourcesCommand,
    list_options: &SourcesListOptions,
) {
    let mut sources = config::get_sources_list();
    if let Some(ref pattern) = list_options.pattern {
        let re = RegexBuilder::new(pattern.as_ref())
            .case_insensitive(true)
            .build()
            .unwrap();
        sources = sources
            .into_iter()
            .filter(|(name, _src)| re.is_match(name))
            .collect();
    }
    let mut max_source_length = 0;
    sources
        .iter()
        .for_each(|src| max_source_length = max(src.0.len(), max_source_length));
    for source in sources {
        println!(
            "{:spacing$}{}",
            source.0,
            source.1.get_type_name(),
            spacing = max_source_length + 2
        );
    }
}

pub fn sources_show(
    _args: &ApplicationArguments,
    _sources_command: &SourcesCommand,
    show_options: &SourcesShowOptions,
) {
    let mut sources = config::get_sources_list();
    if let Some(ref pattern) = show_options.pattern {
        let re = RegexBuilder::new(pattern.as_ref())
            .case_insensitive(true)
            .build()
            .unwrap();
        sources = sources
            .into_iter()
            .filter(|(name, _src)| re.is_match(name))
            .collect();
    }
    let mut max_source_length = 0;
    sources
        .iter()
        .for_each(|src| max_source_length = max(src.0.len(), max_source_length));
    for (name, source) in sources {
        println!("{}\n{}", name, source.to_full_toml())
    }
}

pub fn sources(args: &ApplicationArguments, sources_command: &SourcesCommand) {
    match &sources_command.command {
        SourcesSubCommand::Add(add_options) => sources_add(&args, &sources_command, &add_options),
        SourcesSubCommand::Delete(delete_options) => {
            sources_delete(&args, &sources_command, &delete_options)
        }
        SourcesSubCommand::Edit(edit_options) => {
            sources_edit(&args, &sources_command, &edit_options)
        }
        SourcesSubCommand::List(list_options) => {
            sources_list(&args, &sources_command, &list_options)
        }
        SourcesSubCommand::Show(show_options) => {
            sources_show(&args, &sources_command, &show_options)
        }
    };
}

#[derive(Clone, Debug, Parser)]
pub struct SourcesCommand {
    #[command(subcommand)]
    pub command: SourcesSubCommand,
}

#[derive(Clone, Debug, Parser)]
pub enum SourcesSubCommand {
    #[command(name = "add", about = "add source")]
    Add(SourcesAddOptions),
    #[command(name = "delete", about = "delete source")]
    Delete(SourcesDeleteOptions),
    #[command(name = "edit", about = "edit source definition")]
    Edit(SourcesEditOptions),
    #[command(name = "list", about = "list sources")]
    List(SourcesListOptions),
    #[command(name = "show", about = "show source details")]
    Show(SourcesShowOptions),
}

#[derive(Clone, Debug, Parser)]
pub struct SourcesAddOptions {
    #[arg(help = "source name")]
    pub name: String,
    #[command(subcommand)]
    pub source: SourceConfigCommand,
}

#[derive(Clone, Debug, Parser)]
pub struct SourcesDeleteOptions {
    #[arg(help = "source name")]
    pub name: String,
}

#[derive(Clone, Debug, Parser)]
pub struct SourcesEditOptions {
    #[arg(help = "source name")]
    pub name: String,
}

#[derive(Clone, Debug, Parser)]
pub struct SourcesListOptions {
    #[arg(help = "pattern to search for (using regular expression)")]
    pub pattern: Option<String>,
}

#[derive(Clone, Debug, Parser)]
pub struct SourcesShowOptions {
    #[arg(help = "pattern to search for (using regular expression)")]
    pub pattern: Option<String>,
}
