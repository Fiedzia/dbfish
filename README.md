# dbexport
Export data from relational databse to CSV/JSON/Sqlite file
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
 * sqlite file


example:

```bash
    dbexport export mysql --database users -q 'select * from users' csv somefile.csv
    dbexport export mysql --database users --user joe --password secret -q 'select * from users' sqlite somefile.sqlite
```


Fancy features:

    * progressbar
    * color support
    * truncate long texts


TODO:

    * more sources (sqlite, csv, PostgreSQL, BigQuery, maybe json/Solr/ES)
    * more destinations (fancy html, HDF5, ODF, XLSX)
    * support a bit more MySQL features (few types were ommited)
    * helpful error messages
    * kill all .unwrap()
    * debug destination


Design principles:

    * Keep it simple. This is not a tool that translates every feature of every database perfectly.
    * Verbose errors. If something doesn't work, say it. Swallowing errors silently is not acceptable.


Development:

You will need [link Rust](https://www.rust-lang.org/). I recommend using latest stable version.
Once you have that, running cargo build --release should just work, generating target/release/dbexport binary.
You will also need sqlite3 installed, since its dynamically linked.

