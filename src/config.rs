use std::collections::HashMap;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use lazy_static::lazy_static;
use toml;
use dirs::home_dir;

use crate::commands::common::SourceConfigCommand;

#[cfg(feature = "use_mysql")]
use crate::commands::common::MysqlConfigOptions;
#[cfg(feature = "use_postgres")]
use crate::commands::common::PostgresConfigOptions;
#[cfg(feature = "use_sqlite")]
use crate::commands::common::SqliteConfigOptions;


lazy_static!{
    pub static ref USER_DEFINED_SOURCES: HashMap<String, SourceConfigCommand> = {
        let commands = get_sources_list();
        let mut hm = HashMap::new();
        for (k, v) in commands {
            hm.insert(k, v);
        };
        hm
    };
}


pub fn toml_from_file(filename: &Path) -> std::io::Result<toml::Value> {
    let mut file = std::fs::File::open(filename)?;
    let mut s = String::new();
    file.read_to_string(&mut s)?;
    Ok(s.parse::<toml::Value>().unwrap())
}


pub fn get_config_directory() -> PathBuf {
    home_dir().unwrap().join(".dbexport")
}

pub fn get_sources_config_directory() -> PathBuf {
    home_dir().unwrap().join(".dbexport").join("sources")
}

pub fn get_sources_list() -> Vec<(String, SourceConfigCommand)> {
    let dirname = get_sources_config_directory();
    let mut entries = if std::path::Path::new(&dirname).exists() {
        std::fs::read_dir(dirname)
            .unwrap()
            .map(|entry| entry.unwrap())
            .filter(|entry| !entry.file_type().unwrap().is_dir())
            .map(|entry| {
                let name = entry
                    .file_name()
                    .into_string()
                    .unwrap();
                let toml_value = toml_from_file(&entry.path()).unwrap();
                let data_type = toml_value
                    .as_table()
                    .unwrap()
                    .get("type")
                    .unwrap()
                    .as_str()
                    .unwrap();
                let source_config_command = SourceConfigCommand::from_toml(&toml_value);
                (name, source_config_command)
            }).collect()
    } else {
            vec![]
    };
    entries.sort_by(|a, b| a.0.cmp(&b.0) );
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

    let type_name = source.get_type_name();
    let mut toml_table = toml::value::Table::new();
    toml_table.insert("type".to_string(), toml::Value::String(type_name.clone()));
    toml_table.insert(type_name, source.to_toml());

    let toml_content = toml::Value::Table(toml_table);
    let mut file = std::fs::File::create(filename).unwrap();
    file.write_all(toml::to_string(&toml_content).unwrap().as_bytes()).unwrap();
    file.flush().unwrap();
}
