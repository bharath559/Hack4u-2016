import os
import logging
import redis
import gevent
from flask import Flask, render_template, request
from flask import jsonify
from flask_sockets import Sockets
import urlparse
import psycopg2
from psycopg2.extras import RealDictCursor
import json
from datetime import date, timedelta as td


from flask import Flask
app = Flask(__name__)


urlparse.uses_netloc.append("postgres")
url = urlparse.urlparse("postgres://mzssljxxkxtdaf:1I_Qjdd7L4J1dosIU2v3u7GlQ-@ec2-54-83-0-187.compute-1.amazonaws.com:5432/dbnvmuoqgo65r5")

conn = psycopg2.connect(
    database=url.path[1:],
    user=url.username,
    password=url.password,
    host=url.hostname,
    port=url.port
)


class Profile(object):
    matchingscore=0
    id=0
    interests=[]
    displayname=""
    def __init__(self):
        self.id=1

def databaseConnection():
	urlparse.uses_netloc.append("postgres")
	url = urlparse.urlparse("postgres://mzssljxxkxtdaf:1I_Qjdd7L4J1dosIU2v3u7GlQ-@ec2-54-83-0-187.compute-1.amazonaws.com:5432/dbnvmuoqgo65r5")
	
	connection = psycopg2.connect(
		database=url.path[1:],
		user=url.username,
		password=url.password,
		host=url.hostname,
		port=url.port
	)
	return connection
def getInterestsForUser(userId):
    conn = databaseConnection()
    getUserInterest="select interest_id from user_interest where user_id="+ userId
    cur=conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(getUserInterest)
    currentUserSet = []
    currentUserInterest=[]
    allrows=cur.fetchall()
    for eachinterest in allrows:
        interestId=eachinterest['interest_id']
        currentUserSet.append(interestId)
        userInterestQuery=userInterest ="select name from interest where id="+str(interestId)
        readcur=conn.cursor(cursor_factory=RealDictCursor)
        readcur.execute(userInterestQuery)
        allInterest=readcur.fetchone()
        currentUserInterest.append(allInterest['name'])
    return currentUserInterest

@app.route('/')
def hello():
    return render_template('index.html')
    
@app.route('/test',methods=['GET'])
def testing():
    return jsonify({'test': "1"})
    
    
    
# @app.route('/login',methods=['POST'])
# def doLogin():
	
# 	req=json.loads(str(request.data))
# 	username=req['username']
# 	password=req['password']
# 	checkLoginSql="select id from users where email='"+username+"' AND password='"+password+"'"
# 	conn = databaseConnection()
# 	cur=conn.cursor(cursor_factory=RealDictCursor)
# 	cur.execute(checkLoginSql)
	
# 	if cur.rowcount>0:
# 		return jsonify(id=str(cur.fetchone()),status="success")
# 	else:
# 		return jsonify(id=str(cur.fetchone()),status="failure",errormessage="username or password is incorrect")
	
    
@app.route('/getmatchingprofiles',methods=['GET','POST'])
def getMatchingProfiles():
    req=json.loads(str(request.data))
    userId=req['userid']
    currentUserInterests=getInterestsForUser(userId)
    
    finalJSon=[]
    getAllUsers="select u.id,display_name,major,univ.name AS university from users u JOIN university univ ON(univ.id = u.university_id) where u.id!="+str(userId)
    allUsercur=conn.cursor(cursor_factory=RealDictCursor)
    allUsercur.execute(getAllUsers)
    allRows = allUsercur.fetchall()
    matchingInterestList=[]
    eachUserInterestSet=[]
    getUserInterestIds=[]
    finalInterestingList=[]
    idsList=[]
    matchingList=[]
    displayNamesList=[]
    for row in allRows:
        matchingSet=[]
        eachJson={}
        getUserInterestIds = getInterestsForUser(str(row['id']))
        currentUserInterestsSet= set(currentUserInterests)
        getUserInterestIdsSet= set(getUserInterestIds)
        for name in currentUserInterestsSet.intersection(getUserInterestIdsSet):
            matchingSet.append(name)
        matchingPercentage=(len(matchingSet)*100/len(currentUserInterests))
        idsList.append(row['id'])
        displayNamesList.append(row['display_name'])
        matchingList.append(matchingPercentage)
        
        eachJson['id']=row['id']
        eachJson['displayName']=row['display_name']
        eachJson['major']=row['major']
        eachJson['matchingPercentage']=matchingPercentage
        eachJson['interests']=getUserInterestIds
        eachJson['university']= row['university']   
        
        finalJSon.append(eachJson)
    return jsonify(matches=finalJSon)


@app.route('/getInterests',methods=['POST'])
def getUserInterests():
    req=json.loads(str(request.data))
    getUserInterestList=getInterestsForUser(req['userid'])
    
    return jsonify(interests=getUserInterestList)

@app.route('/getsearchresults',methods=['POST'])
def getProfilesWithInterests():
    req=json.loads(str(request.data))
    keyword= req['keyword']
    keywordQuery="select u.id,i.name,u.display_name,u.major,univ.name AS university from user_interest ui JOIN interest i ON(ui.interest_id = i.id) JOIN users u ON(u.id = ui.user_id) JOIN  university univ ON(univ.id = u.university_id) WHERE i.name like '%"+keyword+"%'"
    conn = databaseConnection()
    cur=conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(keywordQuery)
    searchResults=[]
    for row in cur:
        eachResult={}
        getUserInterestList=getInterestsForUser(str(row['id']))
        eachResult['id']=row['id']
        eachResult['displayName'] =row['display_name']
        eachResult['interests']=getUserInterestList
        eachResult['major']=row['major']
        eachResult['university']= row['university']   
        
        searchResults.append(eachResult)

    return jsonify(matches=searchResults)


@app.route('/getBookReadUsers',methods=['POST'])
def getBookReadUsers():
	req=json.loads(str(request.data))
	userId = req['userId']

	if 'isbn' in req.keys():
		isbn = req['isbn']
		sqlUsersForBook="select user_id AS userId, display_name AS displayName, ub.id, univ.name AS  universityName, u.major from user_book ub JOIN users u ON(u.id = ub.user_id) JOIN book b ON(b.id = ub.book_id) JOIN university univ ON(univ.id = u.university_id) where ub.user_id !=" + str(userId) + " AND isbn = '" + isbn + "'"
	else:
		title = req['title']
		sqlUsersForBook="select user_id AS userId, display_name AS displayName, ub.id, univ.name AS  universityName, u.major from user_book ub JOIN users u ON(u.id = ub.user_id) JOIN book b ON(b.id = ub.book_id) JOIN university univ ON(univ.id = u.university_id) where ub.user_id !=" + str(userId) + " AND b.name like '%" + title + "%'"	


	users = []
	conn = databaseConnection()	

	cur = conn.cursor(cursor_factory=RealDictCursor)
	cur.execute(sqlUsersForBook)
	
	userIds = []
	results = cur.fetchall()
	for row in results:
		user = {}
		user['userId'] = row['userid']
		user['displayName'] = row['displayname']
		user['university'] = row['universityname']
		user['major'] = row['major']
		user['books'] = ''
		users.append(user)

		userIds.append(row['userid'])
		print row
		
		#userInterests = getUserInterests(userIds)
		users = getUsersReadBooks(userIds, users)
		users = getInterestsForUsers(userIds, users)

		# for user in users:
		# 	interests = getInterestsForUser(str(user['userId']))
		# 	user['interests'] = interests
				
	response = {}
	response['matches'] = users

	conn.close()
	return json.dumps(response)

	
def getUsersReadBooks(userIds, users):
	conn = databaseConnection()
	
	strUIds = str(userIds)
	strUIds = strUIds.replace("[", "")
	strUIds = strUIds.replace("]", "")
	sqlUsersOtherBooks="select user_id AS userId, string_agg(name, ', ') AS title from user_book ub JOIN book b ON(b.id = ub.book_id) where user_id IN(" + strUIds + ") GROUP BY user_id"
	cur = conn.cursor(cursor_factory=RealDictCursor)
	cur.execute(sqlUsersOtherBooks)
	results = cur.fetchall()
	books = []
	for row in results:
		books = row['title']

		for user in users:
			if user['userId'] == row['userid']:
				user['books'] = books
	return users
@app.route('/sendmessage',methods=['POST'])
def sendMessage():
    req=json.loads(str(request.data))
    message = req['message']
    epochTime = req['epochTime']
    userId=req['userId']
    displayName=req['displayName']
    conn = databaseConnection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("INSERT INTO  message (message,sent_on,user_id,display_name) VALUES ('" + message + "','" + epochTime + "',"+str(userId)+",'"+displayName+"')")
    conn.commit()
    
    return jsonify(status="success")
    
@app.route('/recievemessage',methods=['GET','POST'])
def recieveMessage():
	conn = databaseConnection()
	cur = conn.cursor(cursor_factory=RealDictCursor)
	cur.execute("select * from message")
	messages=[]
	for row in cur:
		message={}
		message['message']=row['message']
		message['epochTime']=long(float(row['sent_on']))
		message['userId']=row['user_id']
		message['displayName']=row['display_name']
		messages.append(message)
	cur.execute("delete from message")
	conn.commit()

	return jsonify(messageList=messages)

@app.route('/registration',methods=['POST'])
def registration():
	req=json.loads(str(request.data))
	emailId=req['email']
	displayName=req['displayName']
	password=req['password']
	major=req['major']
	university=req['university']
	conn=databaseConnection()

	sql = "select max(id) as id from users"

	cur = conn.cursor(cursor_factory=RealDictCursor)
	cur.execute(sql)
	row = cur.fetchone()
	id = row['id']
	cur.close()
	cur=conn.cursor(cursor_factory=RealDictCursor)    
	cur.execute("INSERT INTO users (id, email,university_id,password,major,display_name) VALUES ("+str(id+1)+",'" +emailId+"',1,'"+password+"','"+major+"','"+displayName+"')")
	conn.commit()

	return jsonify(userid=id+1)


def getInterestsForUsers(userIds, users):
	conn = databaseConnection()

	strUIds = str(userIds)
	strUIds = strUIds.replace("[", "")
	strUIds = strUIds.replace("]", "")
	getUserInterest="select ui.user_id AS userId, string_agg(name, ',') AS interest from user_interest ui JOIN interest i ON(ui.interest_id = i.id) where user_id IN(" + strUIds + ") GROUP BY user_id"
	cur=conn.cursor(cursor_factory=RealDictCursor)
	cur.execute(getUserInterest)
	currentUserSet = []
	currentUserInterest=[]
	allrows=cur.fetchall()
	for row in allrows:

		interests = row['interest'].split(",")

		for user in users:
			if user['userId'] == row['userid']:
				user['interests'] = interests


	conn.close()
	return users


@app.route('/getUsersForCourse',methods=['POST'])
def getUsersForCourse():
	req=json.loads(str(request.data))
	userId = req['userId']
	course = req['course']	
	users = []
	conn = databaseConnection()
	sql="select user_id AS userId, display_name AS displayName, univ.name AS  university, u.major from user_course uc JOIN users u ON(u.id = uc.user_id) JOIN course c ON(c.id = uc.course_id) JOIN university univ ON(univ.id = u.university_id) where uc.user_id !=" + str(userId) + " AND  course like '%" + course + "%'"
	#select user_id AS userId, display_name AS displayName, ub.id from user_book ub JOIN users u ON(u.id = ub.user_id) JOIN book b ON(b.id = ub.book_id) where name = 'The Great Gatsby';
	cur = conn.cursor(cursor_factory=RealDictCursor)
	cur.execute(sql)
	
	userIds = []
	results = cur.fetchall()
	for row in results:
		user = {}
		user['userId'] = row['userid']
		user['displayName'] = row['displayname']
		user['university'] = row['university']
		user['major'] = row['major']
		user['courses'] = ''
		users.append(user)

		userIds.append(row['userid'])
		print row
				
		users = getUsersCourses(userIds, users, course)
		users = getInterestsForUsers(userIds, users)
						
	response = {}
	response['matches'] = users
	
	conn.close()
	return json.dumps(response)

def getUsersCourses(userIds, users, course):
	conn = databaseConnection()
	
	strUIds = str(userIds)
	strUIds = strUIds.replace("[", "")
	strUIds = strUIds.replace("]", "")
	sql = "select user_id AS userId, string_agg(course, ', ') AS courses from user_course uc JOIN course c ON(c.id = uc.course_id) where user_id IN(" + strUIds + ") GROUP BY user_id"



	cur = conn.cursor(cursor_factory=RealDictCursor)
	cur.execute(sql)
	results = cur.fetchall()
	courses = []
	for row in results:
		courses = row['courses']

		for user in users:
			if user['userId'] == row['userid']:
				user['courses'] = courses
	return users


@app.route('/updateInterests',methods=['POST'])
def updateInterests():
	req=json.loads(str(request.data))
	userId = req['userId']
	interests = req['interests']
	
# {"userId": 1, "interests":["", ""]}
	deleteAllUserInterests(userId)

	conn = databaseConnection()

	for interest in interests:

		sql = "insert into user_interest(user_id, interest_id) VALUES (" + str(userId) + ", (select id from interest where name='" + interest + "'))"
		print sql
		cur = conn.cursor(cursor_factory=RealDictCursor)
		cur.execute(sql)
		conn.commit()
	
	conn.close()
						
	response = {}	
	
	return json.dumps(response)

@app.route('/addNewInterest',methods=['POST'])
def addNewInterest():

	req=json.loads(str(request.data))
	interest = req['interest']

	conn = databaseConnection()
	
	sql = "insert into interest(name) VALUES ('" + interest + "')"

	cur = conn.cursor(cursor_factory=RealDictCursor)
	cur.execute(sql)
	conn.commit()

	conn.close()
						
	response = {}	
	
	return json.dumps(response)


def deleteAllUserInterests(userId):
	conn = databaseConnection()
	sql="delete from user_interest where user_id =" + str(userId)
	
	cur = conn.cursor(cursor_factory=RealDictCursor)
	cur.execute(sql)
	conn.commit()
	cur.close()
	conn.close()

@app.route('/login', methods=['POST'])
def login():
	req=json.loads(str(request.data))
	userName = req['userName']
	password = req['password']	
	users = []
	conn = databaseConnection()
	# sql = "select u.id, u.display_name AS displayName, u.major, univ.name AS university from users u JOIN university univ ON(univ.id = u.university_id) WHERE (email='" + userName + "' OR display_name='" + userName + "') AND password='" + password + "'"

	sql = "select u.id, u.display_name AS displayName, u.major, univ.name AS university from users u JOIN university univ ON(univ.id = u.university_id) WHERE email='" + userName + "' AND password='" + password + "'"

	cur = conn.cursor(cursor_factory=RealDictCursor)
	cur.execute(sql)
		
	row = cur.fetchone()

	if row:
		user = {}
		user['userId'] = row['id']
		user['displayName'] = row['displayname']
		user['university'] = row['university']
		user['major'] = row['major']

		cur.close()

		sql = "select string_agg(i.name, ',') AS interests from user_interest ui JOIN interest i ON(ui.interest_id = i.id) JOIN users u ON(u.id = ui.user_id) WHERE u.id = " + str(user['userId']) + " GROUP BY u.id"


		cur = conn.cursor(cursor_factory=RealDictCursor)
		cur.execute(sql)
		interests = []
		row = cur.fetchone()
		if row:
			interests = row['interests'].split(",")
		cur.close()
		conn.close()

		user['interests'] = interests					

		response = {}
		response['user'] = user	
		
		return json.dumps(response)
	else:		
		return jsonify(errormessage="username or password is incorrect")


@app.route('/getAllInterests',methods=['POST'])
def getAllInterests():


	conn = databaseConnection()
	sql = "select string_agg(name, ',') AS interest from interest"
	
	cur = conn.cursor(cursor_factory=RealDictCursor)
	cur.execute(sql)
	row = cur.fetchone()
	interests = row['interest'].split(",")

	cur.close()
	conn.close()
	
	response = {}
	response['interests'] = interests	
	
	return json.dumps(response)

if __name__ == "__main__":
    app.run(debug=True)
