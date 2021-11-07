
import sqlite3, csv, datetime, requests, time

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
    adr1 = "https://rossvyaz.gov.ru/upload/gallery/240/63240_520cb7886da777c87fb4975821b2954cb9246b2d.csv"
    adr2 = "https://rossvyaz.gov.ru/upload/gallery/242/63242_7b8559ff4e0a23950ef6f3b6bfdf5d638492e07b.csv"
    adr3 = "https://rossvyaz.gov.ru/upload/gallery/244/63244_0815d272861d8562fa4dad35da1f35916a4c31ab.csv"
    adr4 = "https://rossvyaz.gov.ru/upload/gallery/246/63246_53eb8e11fd3d75d7ac152409b3f1684374672988.csv"

    if(datetime.datetime.today().weekday() == 0):
        request1 = requests.get(adr1, verify=False)
        request2 = requests.get(adr2, verify=False)
        request3 = requests.get(adr3, verify=False)
        request4 = requests.get(adr4, verify=False)

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