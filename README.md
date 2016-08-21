# csv-to-db
Converts CSV files to database tables

Samples the CSV to detect the name and type of the fields.
Creates a table for each CSV.
Inserts the rows into it.

*Note: Assumes CSV's first row are headers

Usage
  csv-to-mysql.py -i <csvfile|dir> -s <db_server> -d <database> -u <user> -p <pass> -k <samplesize>
Example
  csv-to-mysql.py -i test -d myDb
Options
  <csvfile|dir> can be either a csv file or a dir of csv files
  <db_server> is optional. default is "localhost"
  <user> is optional. default is "root"
  <pass> can be left empty
  <samplesize> is number of rows to sample to determine field type. default is 10
