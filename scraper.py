import requests, re, dateutil.parser, sqlite3, time, random
from bs4 import BeautifulSoup as Soup

DB_FILE = 'data.sqlite'

space = re.compile(r'\n+')
numb = re.compile(r'[^0-9]')
rdate = re.compile(r'[^a-z0-9]')
const = re.compile(r'[^a-z0-9\-]')

conn = sqlite3.connect(DB_FILE)
c = conn.cursor()

headers = {'User-Agent':'Python script gathering data, will poll once a day after an initial dump; contact at: reddit.com/u/hypd09', 'Accept-Encoding': 'gzip', 'Content-Encoding': 'gzip'}

c.execute('DROP TABLE IF EXISTS data')
c.execute("CREATE TABLE IF NOT EXISTS data (sno,name,rank,office,state,end_of_watch,date_of_incident,age,tour,badge_no,military_veteran,cause,weapon,offender,summary,image)")

session = requests.Session()

feed = session.get('https://www.odmp.org/feed',headers=headers)
lastOne = int(feed.text.split('https://www.odmp.org/officer/',maxsplit=1)[1].split('-',maxsplit=1)[0])
print('last one',lastOne)

def clean(s):
    return rex.sub(' ',s).strip()

######
c.execute('SELECT sno FROM data ORDER BY sno DESC')
officerNo = c.fetchone()
print('last done',officerNo)


isLast = False
officerNo = 0 if officerNo is None else officerNo[0]

while not isLast:
    officerNo = officerNo + 1
    site = session.get('https://www.odmp.org/officer/'+str(officerNo), headers=headers)

    siteData = Soup(site.text,'lxml')
    
    if site.status_code == 404:
        if officerNo > lastOne: #last one?
            isLast = True #redundant?
            break
        else:
            print(str(officerNo)+' is 404')
            continue

    if site.status_code != 200:
        print('https error '+ str(site.status_code))
        break
        
    ## begin parsing
    info_left = siteData.find(id='memorial_featuredInfo_left').find('img')['src']
    info_right = siteData.find(id='memorial_featuredInfo_right').text.strip().split('\n')
    bio_info = siteData.find(id='memorial_featuredBody_left').text.strip().split('\n')
    body = siteData.find(id='memorial_featuredBody_right').text.strip()

    sno = officerNo
    name = info_right[1].strip()
    rank = info_right[0].strip()
    office = info_right[2].split(',')[0].strip()
    state = info_right[2].split(',')[1].strip()
    end_of_watch = str(dateutil.parser.parse(info_right[3].split(':')[1].strip()).date())
    date_of_incident = end_of_watch
    age = None
    tour = None
    badge_no = None
    cause = None
    weapon = None
    offender = None
    vet = False
    
    for bio in bio_info:
        bl = bio.lower()
        bd = bio
        if ':' in bd:
            bd = bio.split(':')[1].strip()
            
        if 'age:' in bl:
            age = bd
        if 'tour:' in bl:
            tour = bd
        if 'badge #' in bl:
            badge_no = bd
            badge_no = badge_no if 'available' not in badge_no else None
        if 'cause:' in bl:
            cause = bd
        if 'weapon:' in bl:
            weapon = bd
        if 'incident date:' in bl:
            date_of_incident = str(dateutil.parser.parse(bd).date())
        if 'military veteran' in bl:
            vet = True
        if 'offender' in bl:
            offender = bd

    summary = space.sub('\n',body.replace('\r','\n'))
    summary = '\n'.join(summary.split('\n')[:-1])
    while 'Please contact the following agency to send condolences' in summary:
        summary = '\n'.join(summary.split('\n')[:-1])        
    image = info_left if 'nophoto.jpg' not in info_left else None
    
    data = [sno,name,rank,office,state,end_of_watch,date_of_incident,age,tour,badge_no,vet,cause,weapon,offender,summary,image]
    print(data)
    c.execute('INSERT INTO data VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',data)             
conn.commit()
c.close()
