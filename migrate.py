import MySQLdb
import time
import random
import sys
import progressbar

def process(record):
    record = record.strip().split(';')

    if '@' in record[0]:
        record[0] = record[0].split('@')[0]

    record[2] = dbTime(record[2])
    record[3] = dbTime(record[3])
    counter = 4

    while counter < 9:
        record[counter] = record[counter].replace("\\", "")
        record[counter] = record[counter].replace("'", "\\'")
        record[counter] = record[counter].replace('"', '\\"')
        counter += 1

    return record

def dbTime(epochTime):
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(float(epochTime)))

def insertInDb(table, record, dbConn, fields, values):
    cursor = dbConn.cursor()
    sql = "INSERT INTO " + table + " " + fields + " VALUES " + values

    if table == 'auth_user':
        cursor.execute("SELECT MAX(id) FROM auth_user")
        tmp = cursor.fetchone()[0]

        sqlTmp = sql
        sqlTmp += " ON DUPLICATE KEY UPDATE username='{0}'".format(record[0] + "{0}".format(random.randint(0,10000)))

        cursor.execute(sqlTmp)
        dbConn.commit()
        new = cursor.lastrowid

        if new <= tmp:
            cursor.execute(sql)
            dbConn.commit()
            new = cursor.lastrowid

        return new

    else:
        cursor.execute(sql)
        dbConn.commit()
        return cursor.lastrowid

# def persistRecord(dbConn, sql):
#     cursor = dbConn.cursor()
#     cursor.execute(sql)
#     dbConn.commit()

def startProgressBar(maxval):
    return progressbar.ProgressBar(maxval=maxval, widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage()]).start()

def createAllUsers(filename, tableUser, tableProfile):
    with open(filename, 'r') as records:
        bar = startProgressBar(sum(1 for line in open(filename, 'r')))
        counter = 0

        for record in records:
            bar.update(counter)
            record = process(record)

            if len(record[1]) > 75:
                f = open('exempted.txt', 'a')
                f.write("{0}\n".format(record))
                f.close()
                continue

            dbConn = MySQLdb.connect("localhost","edxapp001","password","edxapp")

            fields = '(username,first_name,last_name,password,email,is_staff,is_active,is_superuser,date_joined,last_login)'
            values = '("{username}","{first_name}","{last_name}","","{email}",0,0,0,"{date_joined}","{last_login}")'.format(username=record[0] if len(record[0]) <= 30 else record[0][:30], first_name=record[4] if len(record[4]) <= 30 else '', last_name=record[5] if len(record[5]) <= 30 else '', email=record[1], date_joined=record[2], last_login=record[3])

            userid = insertInDb(tableUser, record, dbConn, fields, values)

            fields = '(user_id,name,city,country,language,bio,location,meta,courseware,allow_certificate,gender,mailing_address,year_of_birth,level_of_education,goals,profile_image_uploaded_at)'
            values = '({user_id},"{name}","{city}","{country}","{language}",NULL,"","","",1,NULL,NULL,NULL,NULL,NULL,NULL)'.format(user_id=userid, name=record[4]+ " " + record[5], city=record[6], country=record[7], language=record[8])

            insertInDb(tableProfile, record, dbConn, fields, values)
            counter += 1

        bar.finish()
        print('Elapsed time is {0} sec'.format(bar.seconds_elapsed))

def processGmailAccount(record):
    return record[0] if 'gmail' not in record[1] else record[1]

def linkSocialUsers(filename):
    with open(filename, 'r') as records:
        bar = startProgressBar(sum(1 for line in open(filename, 'r')))
        counter = 0

        for record in records:
            bar.update(counter)
            record = record.strip().split(';')
            dbConn = MySQLdb.connect("localhost","edxapp001","password","edxapp")
            cursor = dbConn.cursor()

            sql = "SELECT id, email FROM auth_user WHERE email='{record}'".format(record=processGmailAccount(record))
            cursor.execute(sql)
            dbConn.commit()

            user = cursor.fetchone()

            insertSql = "INSERT INTO social_auth_usersocialauth (user_id,provider,uid,extra_data) VALUES ('{id}', 'google-oauth2', '{record}', '')".format(id=user[0], record=user[1])
            cursor.execute(insertSql)
            dbConn.commit()
            counter += 1

        bar.finish()
        print('Elapsed time is {0} sec'.format(bar.seconds_elapsed))

if __name__ == '__main__':
    # createAllUsers(sys.argv[1], 'auth_user', 'auth_userprofile')
    linkSocialUsers(sys.argv[1])
    print('Elapsed time is {0} sec'.format(time.clock()))
