# dbfish

Dbfish aims to be to your standard database tools what Fish is to Bash:
provide more features out of the box, look better and be easier to use.

Right now it can export data from relational database to CSV/HTML/text/SQLite file, among others.
I've created this because I was frustrated with usability and functionality of out-of-the box database tools.
Seriously, psql and mysql clients should do all that long ago.

usage:

```bash
    dbfish export SOURCE [source options] DESTINATION [destination options]
    dbfish help
```

Sources:

 * MySQL
 * PostgreSQL
 * SQLite

Destinations:

 * CSV
 * HTML (done nicely using bootstrap)
 * ODS (ODS spreadsheet)
 * SQLite file
 * text (classic table)
 * text-vertical (each column in its own line)
 * XLSX (Excel spreadsheet)


Examples:

```bash
    dbfish export mysql --database users -q 'select * from users' csv somefile.csv
    dbfish export mysql --database users --user joe --password secret -q 'select * from users' sqlite -f somefile.sqlite
```


Fancy features:

 * manage database credentials (dbfish sources add mydata sqlite -f my_favourite_file.sqlite; dbfish export mydata ...)
 * progressbar
 * color support
 * truncate long texts
 * show database schema ( ```dbfish schema mydata``` )


TODO: (must-have before calling it usable)

 * helpful error messages
 * kill most of .unwrap()
 * debug source and destination
 * tests
 * jump to database shell (native client or python shell with connection set up)
 


TODO: (nice to have)

 * more sources (CSV, BigQuery, maybe JSON/Solr/ES/MongoDB)
 * more destinations (HDF5)
 * support a bit more MySQL and PostgreSQL features (few types were ommited)
 * kill all .unwrap()
 * compress to zip/tgz (useful for csv/text/html)
 * performance (not a priority, but nice to have)
 * have a concept of source providers to integrate with frameworks


Design principles:

* Keep it simple. This is not a tool that translates every feature of every database perfectly.
* Verbose errors. If something doesn't work, say it. Swallowing errors silently is not acceptable.


Development:

You will need [Rust](https://www.rust-lang.org/). I recommend using latest stable version.
Once you have that, running cargo build --release should just work, generating target/release/dbfish binary.
You will also need SQLite3 libs and C compiler installed, since its being built and linked statically,
disable use_sqlite feature if that's a problem for you.
