use std::collections::HashMap;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use lazy_static::lazy_static;
use toml;
use dirs::home_dir;

use crate::commands::common::SourceConfigCommand;


lazy_static!{
    pub static ref USER_DEFINED_SOURCES: HashMap<String, SourceConfigCommand> = {
        let sources = get_sources_list();
        let mut hm = HashMap::new();
        for (k, v) in sources {
            hm.insert(k, v);
        };
        hm
    };

    pub static ref USER_DEFINED_SOURCES_NAMES: String = {
        let mut s = String::new();
        let mut sources: Vec<String> = USER_DEFINED_SOURCES.keys().cloned().collect();
        sources.sort();
        for (idx, key) in sources.iter().enumerate() {
            s.push_str(key);
            s.push(' ');
            if idx >= 4 && idx % 4 == 0 {
                s.push('\n');
            }
        };
        s
    };
}


pub fn toml_from_file(filename: &Path) -> std::io::Result<toml::Value> {
    let mut file = std::fs::File::open(filename)?;
    let mut s = String::new();
    file.read_to_string(&mut s)?;
    Ok(s.parse::<toml::Value>().unwrap())
}


pub fn get_config_directory() -> PathBuf {
    home_dir().unwrap().join(".dbfish")
}

pub fn get_sources_config_directory() -> PathBuf {
    home_dir().unwrap().join(".dbfish").join("sources")
}

pub fn get_sources_list() -> Vec<(String, SourceConfigCommand)> {
    let dirname = get_sources_config_directory();
    let mut entries = if std::path::Path::new(&dirname).exists() {
        std::fs::read_dir(&dirname)
            .expect(&format!("could not read directory: {:?}", &dirname))
            .map(|entry| entry.expect(&format!("could not process path: {:?}", &dirname)))
            .filter(|entry| !entry.file_type().unwrap().is_dir())
            .map(|entry| {
                let name = entry
                    .file_name()
                    .into_string()
                    .expect(&format!("could not process file: {:?}", entry.file_name()));
                let toml_value = toml_from_file(&entry.path()).expect(&format!("could not parse toml file: {:?}", name));
                let source_config_command = SourceConfigCommand::from_toml(&toml_value);
                (name, source_config_command)
            }).collect()
    } else {
            vec![]
    };
    entries.sort_by(|a, b| a.0.to_lowercase().cmp(&b.0.to_lowercase()) );
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
        .join(".dbfish")
        .join("sources")
        .join(name);

    let toml_content =  source.to_full_toml();
    let mut file = std::fs::File::create(filename).unwrap();
    file.write_all(toml::to_string(&toml_content).unwrap().as_bytes()).unwrap();
    file.flush().unwrap();
}
