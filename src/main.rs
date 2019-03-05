#[macro_use]
extern crate structopt;

use structopt::StructOpt;

pub mod commands;
pub mod config;
pub mod definitions;
pub mod destinations;
pub mod sources;
pub mod utils;

use commands::{ApplicationArguments, Command};


fn main() {
    let clapp_app = ApplicationArguments::clap();
    let clap_matches = clapp_app.get_matches();
    println!("matches: {:?}", clap_matches);
    let args =  ApplicationArguments::from_clap(&clap_matches);

    //let args = ApplicationArguments::from_args();
    match args.command {
        Command::Export(ref export_cmd) => {
            commands::export::export(&args, &export_cmd);
        },
        Command::Sources(ref sources_cmd) => {
            commands::sources::sources(&args, &sources_cmd);
        },
    }
}
