#!/usr/bin/python

import sys
import getopt
import os
import csv
import mysql.connector

def isInt(s):
  try:
    int(s)
    return True
  except:
    return False

def isFloat(s):
  try:
    float(s)
    return True
  except:
    return False

def get_col_type(val):
  if isInt(val):
    return 'BIGINT'
  if isFloat(val):
    return 'DECIMAL'
  else:
    return 'TEXT'

def update_col_type(col_types, col, sampleVal):
  col_type = get_col_type(sampleVal)
  # print col_type + ': ' + sampleVal
  if col in col_types:
    if col_types[col] == 'BIGINT':
      col_types[col] = col_type
    if col_types[col] == 'DECIMAL' and col_type == 'TEXT':
      col_types[col] = col_type
  else:
    col_types[col] = col_type



def guess_col_types(csvfilename, fieldnames, col_types, sampleRows=5):
  with open(csvfilename,'rb') as csvfile:
    reader = csv.DictReader(csvfile, delimiter=',', quotechar='"')
    fieldnames.extend(reader.fieldnames)

    for row in reader:
      sampleRows-=1
      for key in row:
        # print key + ': ' + row[key]
        update_col_type(col_types, key, row[key])

      if sampleRows <= 0:
        break

def get_tablename(csvfilename):
  table = csvfilename

  lastSlash = table.rfind('/')
  if lastSlash > 0:
    table = table[lastSlash+1:]

  lastDot = table.rfind('.')
  if lastDot > 0:
    table = table[:lastDot]

  return table

def create_table(csvfilename, fieldnames, col_types, cnx):
  table = get_tablename(csvfilename)
  createsql = 'CREATE TABLE IF NOT EXISTS `'+table+'` ('
  for i,col in enumerate(fieldnames):
    col_type = col_types[col]
    if i > 0:
      createsql += ','
    createsql += '`'+col+'` '+col_type
    if col_type != 'TEXT':
      createsql += '(20)'
  createsql += ')'

  cursor = cnx.cursor()
  cursor.execute(createsql)
  cnx.commit()

def insert_rows(csvfilename, cnx):
  table = get_tablename(csvfilename)
  insertsql = 'LOAD DATA INFILE "'+csvfilename+'"'
  insertsql += ' INTO TABLE `'+table+'`'
  insertsql += ' COLUMNS TERMINATED BY "," OPTIONALLY ENCLOSED BY "\\""'
  insertsql += ' LINES TERMINATED BY "\\n"'
  insertsql += ' IGNORE 1 LINES'

  cursor = cnx.cursor()
  cursor.execute(insertsql)
  cnx.commit()

def convert(cnx, csvfilename, samplesize):
  print 'converting '+csvfilename+'...'
  col_types = {}
  fieldnames = []

  guess_col_types(csvfilename, fieldnames, col_types, samplesize)
  # print col_types

  create_table(csvfilename, fieldnames, col_types, cnx)

  insert_rows(csvfilename, cnx)


def main(argv):
  csvfile = None
  database = None
  user = 'root'
  password = None
  host = 'localhost'
  samplesize = 10

  usage = 'Usage'
  usage += os.linesep + '  csv-to-mysql.py -i <csvfile|dir> -s <db_server> -d <database> -u <user> -p <pass> -k <samplesize>'
  usage += os.linesep + 'Example'
  usage += os.linesep + '  csv-to-mysql.py -i test -d myDb'
  usage += os.linesep + 'Options'
  usage += os.linesep + '  <csvfile|dir> can be either a csv file or a dir of csv files'
  usage += os.linesep + '  <db_server> is optional. default is "localhost"'
  usage += os.linesep + '  <user> is optional. default is "root"'
  usage += os.linesep + '  <pass> can be left empty'
  usage += os.linesep + '  <samplesize> is number of rows to sample to determine field type. default is 10'
  try:
    shortargs = 'hi:s:d:u:p:k:'
    longargs = ['help','csvfile=','db_server=', 'database=','user=','pass=','samplesize=']
    opts, args = getopt.getopt(argv,shortargs,longargs)
  except getopt.GetoptError:
    print usage
    sys.exit(2)
  for opt, arg in opts:
    if opt in ('-h', '--help'):
      print usage
      sys.exit()
    elif opt in ('-i', '--csvfile'):
      csvfile = arg
    elif opt in ('-d', '--database'):
      database = arg
    elif opt in ('-u', '--user'):
      user = arg
    elif opt in ('-p', '--pass'):
      password = arg
    elif opt in ('-k', '--samplesize'):
      samplesize = arg

  if csvfile == None or database == None:
    print usage
    sys.exit()

  print user,password,database

  cnx = mysql.connector.connect(user=user, password=password, database=database, host=host)

  csvfile = os.path.abspath(csvfile)

  if os.path.isfile(csvfile):
    convert(cnx, csvfile, samplesize)
  else:
    for file in os.listdir(csvfile):
      absfile = csvfile+os.sep+file
      if absfile and absfile.lower().endswith('.csv'):
        convert(cnx, absfile, samplesize)

  cnx.close()



if __name__ == '__main__':
  main(sys.argv[1:])





