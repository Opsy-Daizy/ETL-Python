import os
import sys
import petl
import pymssql
import configparser
import requests
import datetime
import json
import decimal

# retrieve data from the cofiguration file
config = configparser.ConfigParser()
try:
    config.read('configurations.ini')
except Exception as e:
    print("could not read file" + str(e))
    sys.exit()

# read data from configuration file

startDate = config['CONFIG']['startdate']
url = config['CONFIG']['url']
TargServer = config['CONFIG']['server']
TargDatabase = config['CONFIG']['database']


# reqest data from API

try:
    BOCResponse = requests.get(url+startDate)
except Exception as e:
    print("could not request"+ str(e))
    sys.exit

# CREATE list of data arrays

BOCDate =[]
BOCRates = []

# check for status code and process json object
if (BOCResponse.status_code == 200):
    BOCReal = json.loads(BOCResponse.text)

    # extract observation data into colum arrays
    
    for row in BOCReal['observations']:
        BOCDate.append(datetime.datetime.strptime(row['d'], '%Y-%m-%d'))
        BOCRates.append(decimal.Decimal(row['FXUSDCAD']['v']))
    
    
     # create a petl table
    ExchangeRate= petl.fromcolumns([BOCDate, BOCRates], header= ['date', 'rate'])

    #load Expense document


    try:
        expenses = petl.io.xlsx.fromxlsx('Expenses.xlsx',sheet='Github')
    except Exception as e:
        print('could not open expenses.xlsx:' + str(e))
        sys.exit()
    
    #join tables

    expenses = petl.outerjoin(ExchangeRate, expenses, key = 'date')

    #filldown the table

    expenses = petl.filldown(expenses, 'rate')

    #remove columns with no exchange rate

    expenses= petl.select(expenses, lambda rec: rec.USD != None)

    #add a CAD table

    expenses = petl.addfield(expenses, 'CAD', lambda rec: decimal.Decimal(rec.USD) * rec.rate)

    #load to target database

    try:
        dbConnection = pymssql.connect(server=TargServer,database=TargDatabase)
    except Exception as e:
        print('could not connect to database:' + str(e))
        sys.exit()
    #populate database

    try:
        petl.io.todb (expenses,dbConnection,'Expenses')
    except Exception as e:
        print('could not write to database:' + str(e))
    print (expenses)




