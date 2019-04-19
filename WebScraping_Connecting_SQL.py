# -*- coding: utf-8 -*-
"""
Created on Fri Oct  5 14:04:30 2018

@author: ltt's pc
"""
#
#import pandas as pd
import requests
import pymysql
import time
import numpy as np
import json
from zhihu_oauth import ZhihuClient
import os

def get_time(t):
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(t))
def get_liveurl_usingid(id):
    return 'https://api.zhihu.com/lives/'+str(id)
def get_speaker_url(id):
    return 'https://api.zhihu.com/people/'+id
def get_sp_lives_url(id):
    return 'https://api.zhihu.com/lives/special_lists/'+str(id)+'/lives?limit=100'
def get_c_lives_url(id):
    return 'https://api.zhihu.com/lives/special_lists/'+str(id)+'/lives?limit=1000'
def get_duration(l_json):
    if l_json['live_type']=='audio':
        return round(l_json['audio_duration']/60000,2)
    if l_json['live_type']=='video':
        return round(l_json['video']['formal_video_tape']['duration']/60,2)



headers = {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36'
    }
#r.encoding = 'utf-8'
#if r.status_code == 200

r = requests.get(url = 'https://api.zhihu.com/people/016ed6faae26432214aa71ecd7263ff2', headers = headers)
r.status_code
r.text

r = 
#tags
class Tag():
    def __init__(self, t_json):
        self.tag_id = t_json['id']
        self.name = t_json['name']
        self.short_name = t_json['short_name']
        self.created_at = get_time(t_json['created_at'])
        self.great_num = t_json['great_num']
        self.live_num = t_json['live_num']
        self.score = t_json['score']
        self.available_num = t_json['available_num']

#speakers----------------------------

#use the token created by zhihu-oauth package
#D:\Documents\UconnCourses\Projects\Oct_5
TOKEN_FILE = 'token.pkl'

client = ZhihuClient()

if os.path.isfile(TOKEN_FILE):
    client.load_token(TOKEN_FILE)
else:
    client.login_in_terminal()
    client.save_token(TOKEN_FILE)   

#Speaker: class the object and attributes were named the same as form in db
def get_emp(people):#employment
    emp = {}
    for employment in people.employments:
        try:
            if 'company' in employment:
                emp[employment.company.name] = employment.job.name
        except:
            emp[employment.company.name] = 0
    return str(emp)

def get_educ(people):#education
    educ={}
    for education in people.educations:
        if 'school' in education:
            try:
                educ[education.school.name] = education.major.name
            except:
                educ[education.school.name] = 0
    return str(educ)

def get_loc(people):# location
    loc = []
    for location in people.locations:
        loc.append(location.name)
    return str(loc)

def get_btopic(people):# topic in badge
    top = []
    for topic in people.badge.topics:
        top.append(topic.name)
    return str(top)

class Speaker():
    def __init__(self, people):
        self.speaker_id = people.id
        self.gender = people.gender
        self.headline = people.headline
        self.name = people.name
        self.url_token = people.pure_data['data']['url_token']
        self.description = people.description
        self.employments = get_emp(people)
        self.educations = get_educ(people)
        self.locations = get_loc(people)
        self.badge_topics = get_btopic(people)
        self.badge_indentity = people.badge.identity
        self.badge_org = people.badge.org_name
        self.badge_best_answerer = people.badge.is_best_answerer
        self.speaker_type = people.pure_data['data']['user_type']
        self.answer_count = people.answer_count
        self.articles_count = people.articles_count
        self.columns_count = people.columns_count
        self.hosted_live_count = people.hosted_live_count
        self.collection_count = people.collection_count
        self.pin_count = people.pin_count
        self.collected_count = people.collected_count
        self.favorited_count = people.favorited_count
        self.follower_count = people.follower_count
        self.thanked_count = people.thanked_count
        self.voteup_count = people.voteup_count
        self.favorite_count = people.favorite_count
        self.following_count = people.following_count
        self.following_column_count = people.following_column_count
        self.following_question_count = people.following_question_count
        self.following_topic_count = people.following_topic_count
        self.participated_live_count = people.participated_live_count


#zhihuLive: class the object and attributes were named the same as form in db------------------------


class zhihuLive():
    def __init__(self, l_json):
        self.live_id = l_json['id']
        self.description = l_json['description_html']
        self.subject = l_json['subject']
        self.speaker_id = l_json['speaker']['member']['id']
        try:
            self.tag_id = l_json['tags'][0]['id']
        except:
            self.tag_id = None
        self.participants = l_json['seats']['taken']
        self.seats_max = l_json['seats']['max']
        self.fee_amount = l_json['fee']['amount']
        self.fee_original = l_json['fee']['original_price']
        self.unit = l_json['fee']['unit']
        self.in_promotion = l_json['in_promotion']
        self.starts_at = get_time(l_json['starts_at'])
        self.ends_at = get_time(l_json['ends_at'])
        self.duration = get_duration(l_json)
        self.liked_num = l_json['liked_num']
        self.live_type = l_json['live_type']
        self.review_count = l_json['review']['count']
        self.review_score = l_json['review']['score']
        self.feedback_score = l_json['feedback_score']
        self.speaker_message_count = l_json['speaker_message_count']
        try:
            self.cospeakers = l_json['cospeakers'][0]['member']['id']
        except:
            self.cospeakers = None
        self.is_refundable = l_json['is_refundable']
        self.has_authenticated = l_json['has_authenticated']
        self.is_audition_open = l_json['is_audition_open']
        self.anonymous_purchase = l_json['anonymous_purchase']
        self.status = l_json['status']
        self.reply_message_count = l_json['reply_message_count']
        self.attachment_count = l_json['attachment_count']
        self.audition_message_count = l_json['audition_message_count']
        self.vip_only = l_json['vip_only']
    def insert_sql(self):
        sql="insert into live (live_id, description, subject, speaker_id, tag_id, participants, seats_max, fee_amount, fee_original, unit, in_promotion, starts_at, ends_at, liked_num, live_type, review_count, review_score, speaker_message_count, cospeakers, is_refundable, has_authenticated, anonymous_purchase, status, reply_message_count, attachment_count, audition_message_count, vip_only,is_audition_open,feedback_score,duration) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        value = (self.live_id, self.description, self.subject, self.speaker_id, self.tag_id, self.participants, self.seats_max, self.fee_amount, self.fee_original, self.unit, self.in_promotion, self.starts_at, self.ends_at, self.liked_num, self.live_type, self.review_count, self.review_score, self.speaker_message_count, self.cospeakers, self.is_refundable, self.has_authenticated, self.anonymous_purchase, self.status, self.reply_message_count, self.attachment_count, self.audition_message_count,self.vip_only,self.is_audition_open, self.feedback_score, self.duration)
        try:
            zhihudb.cursor.execute(sql, value)
            zhihudb.db.commit()
            print('success')
        except pymysql.Error as e:
            print('Got error {!r}, errno is {}'.format(e, e.args[0]))
            zhihudb.db.rollback()

# get participants of a live and insert to db---------------------------------------
#liveid = 879391231942217728

def get_live_participants(liveid):
    #return 'https://api.zhihu.com/lives/{}/members?limit=10&offset=10'.format(liveid)
    num = 100
    count = 0
    flag = False
    members = []
    #live_members = {}
    url_m = 'https://api.zhihu.com/lives/{}/members?limit=100&offset='.format(liveid)
    while flag == False:
    
        url = url_m+str(count*num)
        r = requests.get(url, headers=headers)
        json_m = r.json()
        
        member_page = [i['member']['id'] for i in json_m['data']]
        #speaker_page = [i['speaker']['member']['id'] for i in json1['data']]
        members= members+member_page
        #speakers = speakers+speaker_page
        flag =json_m['paging']['is_end']#renew the flag
        #print(count)
        count = count+1
        time.sleep(np.random.randint(1,3))
    #live_members[liveid] = members
    return members
    
#members = get_live_participants(liveid)

def participants_insert_sql(live_id, members):
    sql="insert into live_members (live_id, participant_id) values (%s,%s)"
    values = []
    w=open('par_insert.txt','a')
    for i in members:
        values.append((live_id,i))
    try:
        zhihudb.cursor.executemany(sql, values)
        zhihudb.db.commit()
        w.write(live_id+', Success')
        w.write('\n')
    except pymysql.Error as e:
        print(live_id)
        print('Got error {!r}, errno is {}'.format(e, e.args[0]))
        w.write(live_id+', '+str(e))
        w.write('\n')
        zhihudb.db.rollback()
#zhihudb = Mysql('zhihu1030')
#participants_insert_sql(liveid, members)
#class the course: class the object and attributes were named the same as form in db-----------------------------------------------------
class Course():
    def __init__(self, c_json):
        self.course_id = c_json['id']
        self.description = c_json['description']
        self.original_price = c_json['original_price']
        self.purchase_price = c_json['purchase_price']
        self.deductible_price = c_json['deductible_price']
        self.in_promotion = c_json['in_promotion']
        self.discount_description = c_json['discount_description']
        self.liked_num = c_json['liked_num']
        self.live_count = c_json['live_count']
        self.live_member_count = c_json['live_member_count']
        self.subject = c_json['subject']
        self.type = c_json['type']
        self.score = c_json['score']
        try:
            self.promotion_end_time = get_time(c_json['promotion']['end_time'])
        except:
            self.promotion_end_time = None
        try:
            self.promotion_start_time = get_time(c_json['promotion']['start_time'])
        except:
            self.promotion_start_time = None
        try:
            self.promotion_price = c_json['promotion']['price']
        except:
            self.promotion_price = None
        self.speaker_id = c_json['course_info']['speaker_id']
        try:
            self.cospeaker = c_json['course_info']['cospeakers'][0]['member']['id']
        except:
            self.cospeaker = None
        self.subtitle = c_json['course_info']['subtitle']
        self.created_at = get_time(c_json['created_at'])


#Speciallist: class the object and attributes were named the same as form in db--------------------------------------
class Speciallist():
    def __init__(self, sl):
        self.speciallist_id = sl['id']
        self.description = sl['description']
        self.original_price = sl['original_price']
        self.purchase_price = sl['purchase_price']
        self.deductible_price = sl['deductible_price']
        self.in_promotion = sl['in_promotion']
        self.discount_description = sl['discount_description']
        self.liked_num = sl['liked_num']
        self.live_count = sl['live_count']
        self.live_member_count = sl['live_member_count']
        self.subject = sl['subject']
        self.type = sl['type']
        self.score = sl['score']
        try:
            self.promotion_end_time = get_time(sl['promotion']['end_time'])
        except:
            self.promotion_end_time = None
        try:
            self.promotion_start_time = get_time(sl['promotion']['start_time'])
        except:
            self.promotion_start_time = None
        try:
            self.promotion_price = sl['promotion']['price']
        except:
            self.promotion_price = None
        self.created_at = get_time(sl['created_at'])


#courses-lives: This form store the relationship of the course and lives---------------------------------------------------------------
            

def c_live_insert_sql(course,lives):
    sql="insert into course_lives (course_id, live_id) values (%s,%s)"
    values = []
    for i in lives:
        values.append((course,i))
    try:
        zhihudb.cursor.executemany(sql, values)
        zhihudb.db.commit()
        print('success')
    except pymysql.Error as e:
        print(course)
        print('Got error {!r}, errno is {}'.format(e, e.args[0]))
        zhihudb.db.rollback()

#speciallist-lives: This form store the relationship of the speciallist and lives--------------------------------------------------------------


def sp_live_insert_sql(sp,lives):
    sql="insert into speciallist_lives (speciallist_id, live_id) values (%s,%s)"
    values = []
    for i in lives:
        values.append((sp,i))
    try:
        zhihudb.cursor.executemany(sql, values)
        zhihudb.db.commit()
        print('success')
    except pymysql.Error as e:
        print(sp)
        print('Got error {!r}, errno is {}'.format(e, e.args[0]))
        zhihudb.db.rollback()

#insert hot lives---------------------------------------------------------------------
def insert_hot_lives(t, m_lives, w_lives):
    sql="insert into hot_lives (crawl_date, live_id, hot_duration) values (%s,%s,%s)"
    values = []
    for i in m_lives:
        values.append((t, i, 30))
    for i in w_lives:
        values.append((t, i, 7))
    try:
        zhihudb.cursor.executemany(sql, values)
        zhihudb.db.commit()
        print('success')
    except pymysql.Error as e:
        #print(sp)
        print('Got error {!r}, errno is {}'.format(e, e.args[0]))
        zhihudb.db.rollback()

#Class mysql: connect the db, insert into the db------------------------------------------
class Mysql:
    def __init__(self, db_name):
        self.db = pymysql.connect('localhost', 'root', '123456', db=db_name)
        self.cursor = self.db.cursor()
        
    def close(self):
        self.cursor.close()
        self.db.close()
    
    #dt: dictionary: keys are column names, values are inserted values , tb: table name in mysql    
    def insert(self, dt, tb):
        w=open('par_insert.txt','a')
        #dt = l
        #tb = 'live'
        ls = [(k, dt[k]) for k in dt if dt[k]]
        sql = 'insert into %s (' % tb + ','.join([i[0] for i in ls]) + ') values (' + \
                   ','.join(['%r' % i[1] for i in ls]) + ');'
        sql = sql.replace('%','percent')
       
        try:
            self.cursor.execute(sql, dt)
            self.db.commit()
            w.write(str(list(dt.values())[0])+', Success')
            w.write('\n')
            
        except pymysql.Error as e:
            print('Got error {!r}, errno is {}'.format(e, e.args[0]))
            # rollback if error
            w.write(str(list(dt.values())[0])+', '+str(e))
            w.write('\n')
            self.db.rollback()
        
        w.close()
#==============================================================================
#crawl data
    
#lives--------------------------------------------------------------------------
#https://api.zhihu.com/lives?limit=100&offset=100
#get live_ids
num = 10
count = 700
flag = False
lives = []
#speakers = []
while flag == False:

    url = 'https://api.zhihu.com/lives?limit=10&offset='+str(count*num)
    r = requests.get(url, headers=headers)
    json1 = r.json()
    
    live_page = [i['id'] for i in json1['data']]
    #speaker_page = [i['speaker']['member']['id'] for i in json1['data']]
    lives= lives+live_page
    #speakers = speakers+speaker_page
    flag =json1['paging']['is_end']#renew the flag
    count = count+1
    time.sleep(np.random.randint(1,3))


#tags-------------------------------------------------------------------------


zhihudb = Mysql('zhihu1030')

#all tags
for tag in client.live_tags:
    break
    t = Tag(tag.pure_data['cache'])
    t = vars(t)
    zhihudb.insert(t,'tag')
    time.sleep(np.random.randint(1,3))



zhihudb.close()

#courses#---------------------------------------------------------------------
# insert Courses
url = 'https://api.zhihu.com/lives/special_lists?limit=200&subtype=course'
r = requests.get(url, headers=headers)
#r.encoding ='utf-8'
c_json = r.json()

# insert speakers for course s
c_speakers = [i['course_info']['speaker']['member']['id'] for i in c_json['data']]
course_ids = [i['id'] for i in c_json['data']]

zhihudb = Mysql('zhihu1030')

courses = c_json['data']
for c in courses:
    #break
    test = Course(c)
    #z.append([test.end_time])
    test = vars(test)
    zhihudb.insert(test, 'course')

zhihudb.close()
#insert course_lives
c_live = {}
course_live_ids = []
for i in course_ids:
    url = get_c_lives_url(i)#get the url of the course
    r = requests.get(url, headers=headers)
    l_json = r.json()
    lives = l_json['data']
    live_ids = []
    for live in lives:
        live_ids.append(live['id'])
    c_live[i] = live_ids
    course_live_ids = course_live_ids + live_ids
    time.sleep(np.random.randint(1,3))

#insert the relationship of course and lives
for k,v in c_live.items():
    #print(k,v)
    c_live_insert_sql(k,v)


#Speciallist----------------------------------------------------------------------
url = 'https://api.zhihu.com/lives/special_lists?limit=200&subtype=special_list'
r = requests.get(url, headers=headers)

l_json = r.json()
splists = l_json['data']
speciallist_ids = []
for sp in splists:
    #break
    speciallist_ids.append(sp['id'])
    test = Speciallist(sp)
    test = vars(test)
    zhihudb.insert(test, 'speciallist')

#get lives that form a speciallist and store in dict format
sp_live = {}
special_live_ids = []
for i in speciallist_ids:
    url = get_sp_lives_url(i)#get the url of the speciallist
    r = requests.get(url, headers=headers)
    l_json = r.json()
    lives = l_json['data']
    live_ids = []
    for live in lives:
        live_ids.append(live['id'])
    sp_live[i] = live_ids
    special_live_ids = special_live_ids+live_ids
    time.sleep(np.random.randint(1,3))

#insert the relationship of speciallist and lives
for k,v in sp_live.items():
    #print(k,v)
    sp_live_insert_sql(k,v)
    #break

#hot montly/weekly--------------------------------------------------------------
#the hot lives are changed dynamically

url = 'https://api.zhihu.com/lives/hot/monthly?limit=100'
r = requests.get(url, headers=headers)
m_json = r.json()
monthly = m_json['data']
m_ids = [i['id'] for i in monthly]
#m_speakers = [i['speaker']['member']['id'] for i in monthly]

#
url = 'https://api.zhihu.com/lives/hot/weekly?limit=100'
t = get_time(time.time())
r = requests.get(url, headers=headers)
w_json = r.json()
weekly = w_json['data']
w_ids = [i['id'] for i in weekly]
#w_speakers = [i['speaker']['member']['id'] for i in weekly]


zhihudb = Mysql('zhihu1030')

#insert the hot lives with scrap time
   
insert_hot_lives(t, m_ids, w_ids)

zhihudb.close()

hot_live_ids = m_ids+w_ids

#

#insert lives------------------------------------------------------------------
#combine all live ids gathered
lives = set(lives+course_live_ids+special_live_ids+hot_live_ids)
zhihudb = Mysql('zhihu1030')

for i in lives:
    r = requests.get(get_liveurl_usingid(i), headers=headers)#get the url of the lives
    l_json = r.json()
    l = zhihuLive(l_json)
    l = vars(l)
    zhihudb.insert(l,'live')
    #break
    time.sleep(np.random.randint(1,3))

zhihudb.close()

#speakers----------------------------------------------------------------------      
#insert all speakers

zhihudb = Mysql('zhihu1030')
sql = " SELECT speaker_id FROM live"
zhihudb.cursor.execute(sql)
speakers = [column[0] for column in zhihudb.cursor.fetchall()]
speakers = set(speakers)
#s = 'a3a124c97bc93540a3c956c24bc3b465'
for s in speakers:
    people = client.people(s)
    people = Speaker(people)
    test = vars(people)
    zhihudb.insert(test, 'speaker')
    time.sleep(np.random.randint(1,3))

zhihudb.close()
