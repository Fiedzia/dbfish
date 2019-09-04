Code overview:

See files.txt to see what is where and how code is organized

General idea:

We have a set of database sources.
DBFish provides set of commands that operate on those.
All commands are defined using structopt.
Commands can be chained.

Data export:

Since every database has its own idea on how to store and transfer different data types,
DBFish resorts to some common format: src/definitions.rs defines ColumnType and Value types
which every source and destionation will use and understand.
Data source and destinations are structs that implement DataSource/DataDestination traits.

Configuration:

data sources will be stored in ~/.dbfish/sources directory as toml files, one for each source.
