
import sqlite3, csv, datetime, requests, time
from bs4 import BeautifulSoup

startTime = time.time()

db = sqlite3.connect('rossvyaz.db', isolation_level='DEFERRED')
sql = db.cursor()
db.isolation_level = None
sql.execute('''PRAGMA synchronous = OFF''')
sql.execute('PRAGMA temp_store = MEMORY')

def getCSV(filename, request):
    with open(filename, 'wb') as file:
        
        file.write(request.content)
    writeCSV(filename)
    

def writeCSV(filename):
    with open(filename, 'r', encoding='utf-8') as file:
        rdata = csv.DictReader(file, delimiter = ';')
        newfilename = filename[:11] + "_upd.csv"
        with open(newfilename, 'w', encoding= 'utf-8', newline='') as wfile:
            header = ["АВС/ DEF", "От", "До", "Емкость", "Оператор", "Регион"]
            wdata = csv.DictWriter(wfile, fieldnames=header)

            try:
                for row in rdata:
                    wdata.writerow(row)
            except:
                for row in rdata:
                    del row["ИНН"]
                    wdata.writerow(row)


def dowloadDB():
    url = "https://rossvyaz.gov.ru/about/otkrytoe-pravitelstvo/otkrytye-dannee/reestr-otkrytykh-dannykh"
    r = requests.get(url, verify=False)
    soup = BeautifulSoup(r.text, 'lxml')

    adr = soup.find('table').find('tbody').findAll('tr')[1].findAll('td')[2].find('p').findAll('a')

    records = []
    for link in adr:
        records.append(link.get('href'))

    if(datetime.datetime.today().weekday() == 0):
        request1 = requests.get(records[0], verify=False)
        request2 = requests.get(records[1], verify=False)
        request3 = requests.get(records[2], verify=False)
        request4 = requests.get(records[3], verify=False)

        getCSV('csvfiles/r1.csv', request1)
        getCSV('csvfiles/r2.csv', request2)
        getCSV('csvfiles/r3.csv', request3)
        getCSV('csvfiles/r4.csv', request4)


def writeDB(filename):
    query ="INSERT OR IGNORE INTO rossvyaz VALUES (?, ?, ?, ?, ?, ?)"

    transaction = []
    i = 0
    with open(filename, 'r', encoding="utf-8") as file:
        data = csv.reader(file)
        
        for row in data:
            i = i + 1
            transaction.append(row)
            
            # if (row == data[-1]):
            #     sql.execute('BEGIN;')
            #     sql.executemany(query, transaction,)
            #     sql.execute('COMMIT;')

            if (i == 1000):
                i = 0
                sql.execute('BEGIN;')
                sql.executemany(query, transaction)
                sql.execute('COMMIT;')
                

def createDB():
    
    attribute = """
CREATE TABLE IF NOT EXISTS rossvyaz (
    code TEXT,
    ot INTEGER,
    do INTEGER,
    capcity TEXT,
    operator TEXT,
    region TEXT,
    CONSTRAINT UC_Number UNIQUE (code, ot, do)
)"""

    sql.execute(attribute)
    db.commit()

    writeDB('csvfiles/r1_upd.csv')
    writeDB('csvfiles/r2_upd.csv')
    writeDB('csvfiles/r3_upd.csv')
    writeDB('csvfiles/r4_upd.csv')

dowloadDB()
createDB()
endTime = time.time()
print(f"Execution time: {endTime - startTime}")