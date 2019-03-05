use std::io::Write;
use std::path::{Path, PathBuf};

use toml;
use dirs::home_dir;

use crate::commands::SourceConfigCommand;


pub fn get_config_directory() -> PathBuf {
    home_dir().unwrap().join(".dbexport")
}

pub fn get_sources_config_directory() -> PathBuf {
    home_dir().unwrap().join(".dbexport").join("sources")
}

pub fn get_sources_list() -> Vec<String> {
    let dirname = get_sources_config_directory();
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
    entries
}

pub fn ensure_config_directory_exists() {
    if !get_config_directory().exists() {
        std::fs::create_dir(&get_config_directory()).unwrap();
    };
    if !get_sources_config_directory().exists() {
        std::fs::create_dir(&get_sources_config_directory()).unwrap();
    }
}

pub fn save_source_config(name: &str, source: &SourceConfigCommand) {
    ensure_config_directory_exists();
    let filename = home_dir()
        .unwrap()
        .join(".dbexport")
        .join("sources")
        .join(name);

    let type_name = source.get_name();
    let mut toml_table = toml::value::Table::new();
    toml_table.insert("type".to_string(), toml::Value::String(type_name.clone()));
    toml_table.insert(type_name, source.to_toml());

    let toml_content = toml::Value::Table(toml_table);
    let mut file = std::fs::File::create(filename).unwrap();
    file.write_all(toml::to_string(&toml_content).unwrap().as_bytes()).unwrap();
    file.flush().unwrap();

}
