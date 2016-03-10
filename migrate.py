import MySQLdb
import time
import random
import sys
import progressbar
import uuid
from simplecrypt import encrypt

def process(record, duplicate=False):
    record = record.strip().split(';')

    if not duplicate:
        record = getDbTimeForAll(record)

    if '@' in record[0]:
        chopOffDomain(record[0])

    counter = 4
    while counter < 9:
        record[counter].replace("\\", "")
        record[counter].replace("'", "\\'")
        record[counter].replace('"', '\\"')
        counter += 1

    return record

def getDbTimeForAll(record):
    record[2] = dbTime(record[2])
    record[3] = dbTime(record[3])

    return record

def processDuplicateAccounts(filename):
    uniqueAccounts = []
    duplicate_groups = groupDuplicates(filename)
    print('Searching for the most recent account from duplicates per account...')
    bar = startProgressBar(len(duplicate_groups))
    counter = 0

    for group in duplicate_groups:
        bar.update(counter)
        uniqueAccounts.append(getUniqueAccount(group))
        counter += 1

    bar.finish()
    print('Search time was {0} sec.'.format(bar.seconds_elapsed))

    return uniqueAccounts

def getUniqueAccount(duplicates):
    criteria = [9,3,10,2]
    tmp = []
    recentAccount = -1

    for criterion in criteria:
        for duplicate in duplicates:
            tmp.append(int(duplicate[criterion]))

        if sum(tmp) > 0:
            recentAccount = tmp.index(max(tmp))
            break
        else:
            tmp = []

    if recentAccount < 0:
        tmp = []

        for duplicate in duplicates:
            tmp.append(sum(duplicate[criterion] for criterion in criteria))

        recentAccount = tmp.index(max(tmp))

    return duplicates[recentAccount]

def readDuplicatesIntoArray(filename):
    print('Loading data from file...')
    bar = startProgressBar(sum(1 for line in open(filename, 'r')))
    tmp = []
    counter = 0

    with open(filename, 'r') as duplicates:
        for duplicate in duplicates:
            bar.update(counter)
            tmp.append(process(duplicate, True))
            counter += 1

        bar.finish()
        print('Load time was {0} sec.'.format(bar.seconds_elapsed))

    return tmp

def groupDuplicates(filename):
    tmp = readDuplicatesIntoArray(filename)
    start = time.time()
    print('Grouping duplicate records...')
    duplicate_groups = []

    while len(tmp) > 0:
        for record_out in tmp:
            new_tmp = []

            for record_in in tmp:
                if tmp[0][1] == record_out[1]:
                    new_tmp.append(tmp.pop(0))

            if len(new_tmp) != 0:
                duplicate_groups.append(new_tmp)

    print('Grouping time was {0} sec.'.format(time.time() - start))

    return duplicate_groups

def chopOffDomain(email):
    return email.split('@')[0]

def dbTime(epochTime):
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(float(epochTime)))

def static_vars(**kwargs):
    def decorate(func):
        for k in kwargs:
            setattr(func, k, kwargs[k])
        return func
    return decorate

@static_vars(counter=random.randint(0,10000))
def count():
     count.counter += 1
     return count.counter

def startProgressBar(maxval):
    return progressbar.ProgressBar(maxval=maxval, widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage()]).start()

def createAllUsers(filename, tableUser, tableProfile, dbConn):
    records = open(filename, 'r') if type(filename) == str else filename
    print('Creating accounts for all supplied users...')
    bar = startProgressBar(sum(1 for line in open(filename, 'r')) if type(filename) == str else len(filename))
    counter = 0

    for record in records:
        bar.update(counter)

        record = process(record) if type(record) == str else getDbTimeForAll(record)

        populateProfileTable(tableUser, tableProfile, record, dbConn)
        counter += 1

    bar.finish()
    print('Account-creation time was {0} sec.'.format(bar.seconds_elapsed))

    if type(filename) == str:
        records.close()

def populateUsersTable(tableUser, record, dbConn):
    fields = '(username,first_name,last_name,password,email,is_staff,is_active,is_superuser,date_joined,last_login)'
    values = '("{username}","{first_name}","{last_name}","","{email}",0,0,0,"{date_joined}","{last_login}")'.format(username=record[0] if len(record[0]) <= 30 else record[0][:30], first_name=record[4] if len(record[4]) <= 30 else '', last_name=record[5] if len(record[5]) <= 30 else '', email=record[1], date_joined=record[2], last_login=record[3])

    return insertInDb(tableUser, record, dbConn, fields, values)

def populateProfileTable(tableUser, tableProfile, record, dbConn):
    fields = '(user_id,name,city,country,language,bio,location,meta,courseware,allow_certificate,gender,mailing_address,year_of_birth,level_of_education,goals,profile_image_uploaded_at)'
    values = '({user_id},"{name}","{city}","{country}","{language}",NULL,"","","",1,NULL,NULL,NULL,NULL,NULL,NULL)'.format(user_id=populateUsersTable(tableUser, record, dbConn), name=record[4]+ " " + record[5], city=record[6], country=record[7], language=record[8])

    insertInDb(tableProfile, record, dbConn, fields, values)

def populateSocialAuthTable(table, record, dbConn, user, provider):
    fbid = ''

    if provider == 'facebook':
        tmp = record[2].strip().strip('/').split('//www.facebook.com/')

        if 'profile.php?id=' in tmp[1]:
            fbid += tmp[1].split('=')[1]
        elif 'app_scoped_user_id/' in tmp[1]:
            fbid += tmp[1].split('/')[1]
        else:
            fbid += tmp[1]

    fields = '(user_id,provider,uid,extra_data)'
    values = "('{id}', '{provider}', '{uid}', '')".format(id=user[0], provider=provider, uid=user[1] if provider == 'google-oauth2' else fbid)

    insertInDb(table, record, dbConn, fields, values)


def insertInDb(table, record, dbConn, fields, values):
    cursor = dbConn.cursor()
    sql = "INSERT {flag} INTO {table} {field} VALUES {value}".format(flag='IGNORE' if table == 'social_auth_usersocialauth' else '',table=table,field=fields,value=values)

    if table == 'auth_user':

        try:
            cursor.execute("SELECT MAX(id) FROM " + table)
            tmp = cursor.fetchone()[0]

            sqlTmp = sql
            sqlTmp += " ON DUPLICATE KEY UPDATE username='{0}'".format(record[0] + "{0}".format(count()))

            cursor.execute(sqlTmp)
            dbConn.commit()
            new = cursor.lastrowid

            if new <= tmp:
                cursor.execute(sql)
                dbConn.commit()
                new = cursor.lastrowid

            return new
        except:
            last = open('last_record.txt', 'w')
            last.write('{0}'.format(record))

    else:
        try:
            cursor.execute(sql)
            dbConn.commit()
            return cursor.lastrowid
        except:
            last = open('last_record.txt', 'w')
            last.write('{0}'.format(record))

def getSocialAccountEmail(record, provider):
    if provider == 'google-oauth2':
        return record[0] if 'gmail' not in record[1] and 'googlemail' not in record[1] else record[1]
    elif provider == 'facebook':
        return record[1]

def linkGoogleUsers(filename, dbConn):
    print('Linking Google-auth accounts...')
    linkSocialUsers(filename, dbConn, 'google-oauth2')

def linkFacebookUsers(filename, dbConn):
    print('Linking Facebook-auth accounts...')
    linkSocialUsers(filename, dbConn, 'facebook')

def linkSocialUsers(filename, dbConn, provider):
    bar = startProgressBar(sum(1 for line in open(filename, 'r')))
    counter = 0
    notFound = open('not_found.txt', 'a')

    with open(filename, 'r') as records:
        for record in records:
            bar.update(counter)
            record = record.strip().split(';')
            cursor = dbConn.cursor()

            email = getSocialAccountEmail(record, provider)
            sql = "SELECT id, email FROM auth_user WHERE email='{email}'".format(email=email)

            if provider == 'google':
                sql += " OR username='{username}'".format(username=chopOffDomain(email))

            cursor.execute(sql)
            dbConn.commit()

            user = cursor.fetchone()
            counter += 1

            if user is None:
                notFound.write('{0}\n'.format(record))
                continue

            populateSocialAuthTable('social_auth_usersocialauth', record, dbConn, user, provider)

        bar.finish()
        print('Linking time was {0} sec.'.format(bar.seconds_elapsed))

def dbConnect():
    db = {  'host':     'localhost', \
            'user':     'edxapp001', \
            'pass': 'password', \
            'db':       'edxapp', \
            'port':     3306
        }

    return MySQLdb.connect(host=db['host'], user=db['user'], passwd=db['pass'], db=db['db'], port=db['port'])

if __name__ == '__main__':
    print('The program has started!')
    dbConn = dbConnect()

    if sys.argv[1] == '--create-accounts':
        createAllUsers(sys.argv[2], 'auth_user', 'auth_userprofile', dbConn)

    if sys.argv[1] == '--create-unique-from-duplicate':
        createAllUsers(processDuplicateAccounts(sys.argv[2]), 'auth_user', 'auth_userprofile', dbConn)

    if sys.argv[1] == '--link-to-google':
        linkGoogleUsers(sys.argv[2], dbConn)

    if sys.argv[1] == '--link-to-facebook':
        linkFacebookUsers(sys.argv[2], dbConn)
