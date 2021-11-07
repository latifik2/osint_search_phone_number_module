import sqlite3, csv, re, time

startTime = time.time()

db = sqlite3.connect('rossvyaz.db', isolation_level='DEFERRED')
sql = db.cursor()
db.isolation_level = None
sql.execute('''PRAGMA synchronous = OFF''')
sql.execute('PRAGMA temp_store = MEMORY')

def queryDB():
    _input = input("Type phone number: ").strip()
    print(_input)

    pattern = r"((\+7)|8)(\s*)(\(*\d{3}\)*)(\s*\d{3}(\-|\s)*\d{2}(\-|\s)*\d{2})"
    number = ""
    code = ""
    if(re.match(r'(\+7)*8*\d{10}', _input) != None):
        

        

        if(_input[0] == '+'):
            for i in range (5, 12):
                number += _input[i]
            for i in range (2, 5):
                code += _input[i]
        else:
            for i in range(4, 11):
                number += _input[i]
            for i in range (1, 4):
                code += _input[i]
        number = int(number)
        code = int(code)
        #print(number)
        #print(code)

        query = f"""SELECT * FROM rossvyaz WHERE code = {code}
        AND ot <= {number} AND do >= {number}"""

        sql.execute(query)
        data = sql.fetchall()
        #print(data)
        print(f"ОПЕРАТОР: {data[0][4]}\nРЕГИОН: {data[0][5]}")


queryDB()
db.close() 

endTime = time.time()
print(f"Execution time: {endTime - startTime}")