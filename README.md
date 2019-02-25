# dbexport
Export data from relational databse to CSV/JSON/Sqlite file

Sources:

* mysql

destinations:

 * csv
 * text-vertical
 * sqlite

usage:

    dbexport export SOURCE [source options] DESTINATION [destination options]
    dbexport help

example:

    dbexport export mysql --database users -q 'select * from users' csv file.csv
    dbexport export mysql --database users --user joe --password secret -q 'select * from users' sqlite file.sqlite

Fancy features:

    * progressbar
    * color support (automatically enabled when writing to terminal)


TODO:

    * more sources (sqlite, csv, postgresql, bigquery, json)
    * more destinations (fancy html, text, HDF5)
    * support a bit more mysql features (few types were ommited)
    * helpful error messages
    

    
