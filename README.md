# dbfish

Dbfish aims to be to your standard database tools what Fish is to Bash:
provide more features out of the box, look better and be easier to use.

Main features:

* Export data to CSV, HTML, JSON, text, SQLite
* Manage database credentials
* Jump to database shell
* Jump to python environment with connection being set up for you
* display and search database schema with one command


Right now it can export data from relational database to CSV/HTML/text/SQLite file, among others.
I've created this because I was frustrated with usability and functionality of out-of-the box database tools.
Seriously, psql and mysql clients should do all that long ago.

Usage:

```bash

    # define data source named "mydata" which will connect to a database you use (pick one you like)

    dbfish sources add mydata mysql --user joe --password secret
    dbfish sources add mydata postgres --user joe --password secret
    dbfish sources add mydata sqlite /tmp/somefile.sqlite3

    # export your data

    dbfish mydata -q 'select * from sometable' export html /tmp/output.html
    dbfish mydata -q 'select * from sometable' export csv /tmp/output.csv
    dbfish mydata -q 'select * from sometable' export json /tmp/output.json

    # list all available sources and commands

    dbfish help

    # jump to database shell. With added benefits
    # most notable dbfish can use pgcli tools suite
    # and create python environment for any custom needs you may have
    # dbfish supports mysql, psql, python, litecli/mycli/pgcli, sqlite

    dbfish mydata shell # use default shell
    dbfish mydata shell -c mycli  # use mycli shell
    dbfish mydata shell -c python  # use ipython as shell

        Variables and functions:
            conn: database connection
            cursor: connection cursor
            get_conn(): obtain database connection
            msg: function printing this message
        
        Python 3.6.7 (default, Oct 22 2018, 11:32:17) 
        Type 'copyright', 'credits' or 'license' for more information
        IPython 7.4.0 -- An enhanced Interactive Python. Type '?' for help.
        
        In [1]: conn.execute('select * from sometable') 

    # inspect schema

    dbfish mydata schema
    dbfish mydata schema -q user   # display all parts of database schema that contain phrase "user"
    dbfish mydata schema -r -q '201[89]' SOURCE [source options] # display all parts of database schema that match given regex

    dbfish sources add | edit | list | remove # manage database credential
```

Sources:

 * MySQL
 * PostgreSQL
 * SQLite

Destinations:

 * CSV
 * JSON
 * HTML (done nicely using Bootstrap)
 * ODS (ODS spreadsheet)
 * SQLite file
 * text (classic table)
 * text-vertical (each column in its own line)
 * XLSX (Excel spreadsheet)
 * Parquet


Examples:

```bash
    dbfish mysql --database users -q 'select * from users' export csv somefile.csv
    dbfish mysql --database users --user joe --password secret -q 'select * from users' export sqlite -f somefile.sqlite
```


Fancy features:

 * manage database credentials (dbfish sources add mydata sqlite -f my_favourite_file.sqlite; dbfish export mydata ...)
 * progressbar
 * color support
 * truncate long texts
 * show database schema ( ```dbfish schema mydata``` )
 * can be compiled to a single binary with no dependencies (statically linked with musl)
 * use python or mycli/litecli/pgcli as shell

TODO: (must-have before calling it usable)

 * helpful error messages
 * kill all .unwrap()
 * debug source
 * tests
 * documentation. Maybe a video


TODO: (nice to have)

 * more sources (BigQuery, maybe JSON/Solr/ES/MongoDB, SurealDB, DuckDB)
 * more destinations (HDF5, Parquet, Feather)
 * support a bit more MySQL and PostgreSQL features (few types were ommited)
 * kill all .unwrap()
 * compress to zip/tgz (useful for csv/text/html)
 * performance (not a priority, but nice to have)
 * have a concept of source providers to integrate with frameworks
 * add command for user management
 * add command to display database/table sizes
 * add command to show currently running queries
 * add watch command to show periodically query result
 * query benchmark with run statistics
 * db schema diagrams
 * in some distant future, GUI mode maybe
 * allow to use PRQL instead of SQL
 * combine result view with file monitoring via inotify (ala watch command)

 Known issues:

 * parquet requires strict schema definition, sqlite says every column type is binary,
   even for tables created in strict mode (```dbfish sqlite -q 'select 1' export debug - ```
   always reports binary column definition).
   We need to inspect first batch of results to determine type.

Design principles:

* Keep it simple. This is not a tool that translates every feature of every database perfectly.
* Verbose errors. If something doesn't work, say it. Swallowing errors silently is not acceptable.


Development:

You will need [Rust](https://www.rust-lang.org/). I recommend using latest stable version.
Once you have that, running cargo build --release should just work, generating target/release/dbfish binary.
You will also need SQLite3 libs and C compiler installed, since its being built and linked statically,
disable use_sqlite feature if that's a problem for you.

If you want to link it statically, install musl and musl-dev and follow [this guide](https://doc.rust-lang.org/nightly/edition-guide/rust-2018/platform-and-target-support/musl-support-for-fully-static-binaries.html).


![Rust](https://github.com/Fiedzia/dbfish/workflows/Rust/badge.svg)
