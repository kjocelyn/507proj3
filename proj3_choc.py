import sqlite3
import csv
import json
import re

# proj3_choc.py
# You can change anything in this file you want as long as you pass the tests
# and meet the project requirements! You will need to implement several new
# functions.

# Part 1: Read data from CSV and JSON into a new database called choc.db
DBNAME = 'choc.db'
BARSCSV = 'flavors_of_cacao_cleaned.csv'
COUNTRIESJSON = 'countries.json'

def init_db():
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()
    statement = '''
        DROP TABLE IF EXISTS 'Bars';
        '''
    cur.execute(statement)
    statement = '''
        DROP TABLE IF EXISTS 'Countries';
        '''
    cur.execute(statement)
    conn.commit()
    statement = '''CREATE TABLE "Bars" (
    	"Id"	INTEGER,
    	"Company"	TEXT,
    	"SpecificBeanBarName"	TEXT,
    	"REF"	TEXT,
    	"ReviewDate"	TEXT,
    	"CocoaPercent"	REAL,
    	"CompanyLocationId"	INTEGER,
    	"Rating"	REAL,
    	"BeanType"	TEXT,
    	"BroadBeanOriginId"	INTEGER,
		FOREIGN KEY("BroadBeanOriginId") REFERENCES "Countries"("Id"),
        PRIMARY KEY("Id"),
        FOREIGN KEY("CompanyLocationId") REFERENCES "Countries"("Id")
    );
    '''
    conn.execute(statement)
    insert_stuff()
    statement = '''CREATE TABLE "Countries"(
        "Id" INTEGER,
        "Alpha2" TEXT,
        "Alpha3" TEXT,
        "EnglishName" TEXT,
        "Region" TEXT,
        "Subregion" TEXT,
        "Population" INTEGER,
        "Area" Real,
        PRIMARY KEY("Id")
        );
        '''
    conn.execute(statement)
    insert_stuff_2()
    conn.commit()
    conn.close()


def insert_stuff():
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()
    countries = json.load(open(COUNTRIESJSON))
    countries_id_dict = {}
    countries_id_list = []
    bean_origin_id_list = []
    id = 1
    i =0
    for country in countries:
        countries_id_dict[str(country["name"])] = id
        id += 1
    with open(BARSCSV) as BARS:
        csvReader = csv.reader(BARS)
        next(csvReader)
        for row in csvReader:
            countries_id_list.append(countries_id_dict[row[5]])
            if row[8] != "Unknown":
                bean_origin_id_list.append(countries_id_dict[row[8]])
            else:
                bean_origin_id_list.append(row[8])
            row[4] = str(float(row[4].replace("%", ""))/100.0)
            insertion = (None, row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8])
            statement = 'INSERT INTO "BARS" '
            statement += 'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
            cur.execute(statement, insertion)
    for i in range(len(countries_id_list)):
        statement = "UPDATE Bars SET CompanyLocationId=?, BroadBeanOriginId=?"
        statement += "WHERE Id=?"
        insertion = (countries_id_list[i], bean_origin_id_list[i], i+1)
        cur.execute(statement, insertion)
    conn.commit()
    conn.close()


def insert_stuff_2():
    conn = sqlite3.connect(DBNAME)
    cur =conn.cursor()
    with open(COUNTRIESJSON, 'r') as COUNTRIES:
        jsondata = json.loads(COUNTRIES.read())
        for country in jsondata:
            insertion = (None, country["alpha2Code"], country["alpha3Code"], country["name"], country["region"], country["subregion"], country["population"], country["area"])
            statement = 'INSERT INTO "COUNTRIES" '
            statement += 'VALUES (?,?,?,?,?,?,?,?)'
            cur.execute(statement, insertion)
    conn.commit()
    conn.close()


init_db()


# Part 2: Implement logic to process user commands
def process_command(command):
    valid_params = ["ratings", "sellcountry", "sourcecountry", "sellregion", "sourceregion", "cocoa", "top", "bottom", "bars_sold", "sellers", "sources", "region"]
    command_split = command.split()
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()
    result_list = []
    def bars(params):
        country_region = ""
        seller_source = "CompanyLocationId"
        sortby = "Rating"
        top_bottom = " DESC "
        limit = "10"
        for param in params:
            if "sellcountry" in param or "sourcecountry" in param:
                if "sell" in param:
                    seller_source = "CompanyLocationId"
                elif "source" in param:
                    seller_source = "BroadBeanOriginId"
                alpha2 = param.split("=")[1].upper()
                country_region = 'WHERE Countries.Alpha2 = "' + alpha2+'" '
            elif "sellregion" in param or "sourceregion" in param:
                if "sell" in param:
                    seller_source = "CompanyLocationId"
                elif "source" in param:
                    seller_source = "BroadBeanOriginId"
                region_name = param.split("=")[1].lower().capitalize()
                country_region = 'WHERE Countries.Region = "' + region_name+'" '
            if param == "cocoa":
                sortby = "CocoaPercent"
            if "top=" in param or "bottom=" in param:
                limit = param.split("=")[1]
            if "bottom=" in param:
                top_bottom = " ASC "
        sql = 'SELECT SpecificBeanBarName, Company, (SELECT Countries.EnglishName FROM Countries WHERE Countries.Id=Bars.CompanyLocationId), Rating, CocoaPercent, (SELECT Countries.EnglishName FROM Countries WHERE Countries.Id=BroadBeanOriginId) FROM Bars JOIN Countries ON '+ seller_source + '=Countries.Id '
        sql += country_region + "ORDER BY " + sortby + top_bottom + " LIMIT " + limit
        cur.execute(sql)
        rows = cur.fetchall()
        for row in rows:
            result_list.append(row)
    def companies(params):
        agg = "ROUND(AVG(Rating), 1)"
        country_region = ""
        top_bottom =" DESC "
        limit = "10"
        for param in params:
            if "country" in param:
                alpha2 = param.split("=")[1].upper()
                country_region = 'WHERE Countries.Alpha2 = "' + alpha2+'" '
            elif "region" in param:
                region_name = param.split("=")[1].lower().capitalize()
                country_region = 'WHERE Countries.Region = "' + region_name+'" '
            if param == "ratings":
                agg = "ROUND(AVG(Rating), 1)"
            elif param == "cocoa":
                agg = "ROUND(AVG(CocoaPercent), 2)"
            elif param == "bars_sold":
                agg = "COUNT(SpecificBeanBarName)"
            if "top=" in param or "bottom=" in param:
                limit = param.split("=")[1]
            if "bottom=" in param:
                top_bottom = " ASC "
        sql = 'SELECT Company, Countries.EnglishName, '+ agg + ' FROM Bars JOIN Countries ON CompanyLocationId = Countries.Id '
        sql += country_region + 'GROUP BY Company HAVING Count(SpecificBeanBarName)>4 '
        sql += 'ORDER BY ' + agg + top_bottom + ' LIMIT ' + limit
        cur.execute(sql)
        rows = cur.fetchall()
        for row in rows:
            result_list.append(row)
    def countries(params):
        agg = "ROUND(AVG(Rating),1)"
        region = ""
        seller_source = "CompanyLocationId"
        top_bottom = " DESC "
        limit = "10"
        for param in params:
            if "region" in param:
                region = 'WHERE Countries.Region ="' + param.split("=")[1].lower().capitalize()+ '" '
            if "sellers" in param:
                seller_source = "CompanyLocationId"
            elif "sources" in param:
                seller_source = "BroadBeanOriginId"
            if param == "ratings":
                agg = "ROUND(AVG(Rating), 1)"
            elif param == "cocoa":
                agg = "ROUND(AVG(CocoaPercent), 1)"
            elif param == "bars_sold":
                agg = "COUNT(SpecificBeanBarName)"
            if "top=" in param or "bottom=" in param:
                limit = param.split("=")[1]
            if "bottom=" in param:
                top_bottom = " ASC "
        sql = 'SELECT Countries.EnglishName, Countries.Region, '+ agg + ' FROM Bars JOIN Countries ON ' + seller_source +'= Countries.Id '
        sql += region + 'GROUP BY ' + seller_source + ' HAVING Count(SpecificBeanBarName)>4 '
        sql += 'ORDER BY ' + agg + top_bottom + ' LIMIT ' + limit
        cur.execute(sql)
        rows = cur.fetchall()
        for row in rows:
            result_list.append(row)
    def regions(params):
        agg = "ROUND(AVG(Rating),1)"
        seller_source = "CompanyLocationId"
        top_bottom = " DESC "
        limit = "10"
        for param in params:
            if "sellers" in param:
                seller_source = "CompanyLocationId"
            elif "sources" in param:
                seller_source = "BroadBeanOriginId"
            if param == "ratings":
                agg = "ROUND(AVG(Rating), 1)"
            elif param == "cocoa":
                agg = "ROUND(AVG(CocoaPercent), 1)"
            elif param == "bars_sold":
                agg = "COUNT(SpecificBeanBarName)"
            if "top=" in param or "bottom=" in param:
                limit = param.split("=")[1]
            if "bottom=" in param:
                top_bottom = " ASC "
        sql = 'SELECT Countries.Region, '+ agg + ' FROM Bars JOIN Countries ON ' + seller_source + '= Countries.Id '
        sql += 'GROUP BY Countries.Region HAVING Count(SpecificBeanBarName)>4 '
        sql += 'ORDER BY ' + agg + top_bottom + ' LIMIT ' + limit
        cur.execute(sql)
        rows = cur.fetchall()
        for row in rows:
            result_list.append(row)

    params = command_split[1:]
    if command_split[0] == "bars":
        bars(params)
    elif command_split[0] == "companies":
        companies(params)
    elif command_split[0] == "countries":
        countries(params)
    elif command_split[0] == "regions":
        regions(params)

    return result_list

def load_help_text():
    with open('help.txt') as f:
        return f.read()

#Part 3: Implement interactive prompt. We've started for you!
def interactive_prompt():
    help_text = load_help_text()
    response = ''
    while response != 'exit':
        response = input('Enter a command: ')
        valid_params = ["ratings", "sellcountry", "sourcecountry", "sellregion", "sourceregion", "cocoa", "top", "bottom", "bars_sold", "sellers", "sources", "region"]
        commands = ["bars", "companies", "countries", "regions"]
        response_split = re.split(r'[=;,\s]\s*', response)
        if response == 'help':
            print(help_text)
            continue
        elif response == "exit":
            pass
        elif response == "":
            continue
        elif response_split[0] in commands:
            if len(response_split) == 1:
                get_results = process_command(response)
            elif response_split[1] not in valid_params:
                print("Command not recognized: " + response)
                continue
            get_results = process_command(response)
            for result in get_results:
                if len(result) == 6:
                    beanbar = result[0][:12] + "..." if len(result[0]) >12 else result[0]
                    company = result[1][:12] + "..." if len(result[1]) >12 else result[1]
                    companyLocation = result[2][:12] + "..." if len(result[2]) >12 else result[2]
                    roundToOneDec = round(result[3],1)
                    if result[5] == None:
                        sourceCountry = "Unknown"
                    else:
                        sourceCountry = result[5][:12] + "..." if len(result[5]) >12 else result[5]
                    cocoatopercent = str(int(result[4] * 100.0)) + "%"
                    print (f'{beanbar:15} {company:15} {companyLocation:15} {str(roundToOneDec):5} {cocoatopercent:5} {sourceCountry:12}')
                if len(result) == 3:
                    company = result[0][:12] + "..." if len(result[0]) > 12 else result[0]
                    companyLocation = result[1][:12] + "..." if len(result[1]) > 12 else result[1]
                    if result[2] <=1:
                        ratingOrCocoa = str(int(result[2] * 100.0)) + "%"
                    else:
                        ratingOrCocoa = round(result[2],1)
                    print(f'{company:15} {companyLocation:15} {ratingOrCocoa}')
                if len(result) == 2:
                    print(f'{result[0]:12} {result[1]}')
            continue
        else:
            print("Command not recognized: " + response)
    print("bye")
#Make sure nothing runs or prints out when this file is run as a module
if __name__=="__main__":
    interactive_prompt()
