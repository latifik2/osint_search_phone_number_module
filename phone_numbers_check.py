import sqlite3, csv, re

def createDB():
    db = sqlite3.connect('rossvyaz.db')
    sql = db.cursor()

    attribute = """
CREATE TABLE IF NOT EXISTS rossvyaz (
    code TEXT,
    ot INTEGER,
    do INTEGER,
    capcity TEXT,
    operator TEXT,
    region TEXT
)"""

    sql.execute(attribute)
    db.commit()

    with open('rossvyaz1.csv', 'r', encoding="utf-8") as file:
        data = csv.reader(file, delimiter=";")
        
        for row in data:
            sql.execute("INSERT OR IGNORE INTO rossvyaz VALUES (?, ?, ?, ?, ?, ?)", row,)
            db.commit()



    db.close()


def queryDB():
    _input = input("Type phone number: ").strip()
    print(_input)

    number = ""
    code = ""
    if(re.match(r'(\+7)*8*\d{10}', _input) != None):
        db = sqlite3.connect('rossvyaz.db')
        sql = db.cursor()

        

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
        print(number)
        print(code)

        query = f"""SELECT * FROM rossvyaz WHERE code = {code}
        AND ot <= {number} AND do >= {number}"""

        sql.execute(query)
        data = sql.fetchall()
        print(data)

    

    


#createDB()
queryDB()