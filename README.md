# dbexport
Export data from relational databse to CSV/text/Sqlite file, among others.
I've created this because I was frustrated with usability and functionality of out-of-the box database tools.
Seriously, psql and mysql clients should do all that long ago. They still don't.

usage:

```bash
    dbexport export SOURCE [source options] DESTINATION [destination options]
    dbexport help
```

Sources:

 * MySQL


Destinations:

 * csv
 * text (classic table)
 * text-vertical (each column in its own line vertically)
 * ods (ODS spreadsheet)
 * sqlite file
 * xlsx (XLSX spreadsheet)


Examples:

```bash
    dbexport export mysql --database users -q 'select * from users' csv somefile.csv
    dbexport export mysql --database users --user joe --password secret -q 'select * from users' sqlite somefile.sqlite
```


Fancy features:

 * progressbar
 * color support
 * truncate long texts


TODO:

 * more sources (sqlite, CSV, PostgreSQL, BigQuery, maybe json/Solr/ES)
 * more destinations (fancy html, HDF5)
 * support a bit more MySQL features (few types were ommited)
 * helpful error messages
 * kill all .unwrap()
 * debug source/destination
 * tests
 * config file for storing database credentials

Design principles:

* Keep it simple. This is not a tool that translates every feature of every database perfectly.
* Verbose errors. If something doesn't work, say it. Swallowing errors silently is not acceptable.


Development:

You will need [Rust](https://www.rust-lang.org/). I recommend using latest stable version.
Once you have that, running cargo build --release should just work, generating target/release/dbexport binary.
You will also sqlite3 libs and C compiler installed, since its being built and linked statically,
disable use_sqlite feature if that's a problem for you.
