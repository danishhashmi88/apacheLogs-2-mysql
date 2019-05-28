
"""
USAGE:

Basic
index.py apacheAccessLogPath tableName

Advanced (Will create a table of said name in MySql and update the data in it)
index.py apacheAccessLogPath tableName host database user pass

This script helps you to convert your apache logs into mysql, that can be inserted into mysql for further monitoring and quering.

"""

import sys
import datetime
import mysql.connector

class Logs:
	def __init__(self, accessLogPath, tableName, host=None, database=None, user= None, password=None):
		self.accessLogPath 	= accessLogPath
		self.tableName 		= tableName
		self.host 			= host
		self.database 		= database
		self.user 			= user
		self.password 		= password
		self.install = False
		if host!=None and database!=None and user!=None and password!=None:
			try:
				
				self.mydb = mysql.connector.connect(
				  host=host,
				  user=user,
				  passwd=password
				)
				self.install = True
			except:
				print ('could not connect to mysql')
				sys.exit(1)
		
		self.readLog()
	
	def getDatabaseLink(self):
		self.mydb = mysql.connector.connect(
		  host=self.host,
		  user=self.user,
		  passwd=self.password,
		  database=self.database
		)
		self.install = True
	
	def readLog(self):
		try:
			self.file = open(self.accessLogPath, 'r')
		except IOError:
			print ("You must specify a valid file to parse")
			print (__doc__)
			sys.exit(1)
		
		self.getData()
		self.file.close()

	def readLines(self, line):
		row = line.split()
		if len(row)>=10:
			date_string = row[3].replace("[", "")
			_date = datetime.datetime.strptime(date_string, '%d/%b/%Y:%H:%M:%S')
        
			return row[8],{'host': row[0],
				'date': _date.strftime('%Y-%m-%d %H:%M:%S'),
				'type': row[5].replace('"', ""),
				'url': row[6].replace("'", r"\'"),
				'status': row[8],
				'size': row[9],
			}
		else:
			return 0,{}
	
	def getData(self):
		query = []
		sql	 = ""
		for line in self.file:
			status, data = self.readLines(line)
			
			if status != 0:
				query.append("(NULL, '"+data['host']+"', '"+data['date']+"', '"+data['type']+"', '"+data['url']+"', "+data['status']+", '"+data['size']+"')")
		
		
		if self.install==True:
			sql	+= 'INSERT INTO `%s` (`id`, `ip`, `request_date`, `type`, `url`, `status`, `size`) VALUES ' % self.tableName
			sql += ",".join(query)
			sql += ';'
			self.pushData(sql)
		else:
			sql	+= 'INSERT INTO `%s` (`id`, `ip`, `request_date`, `type`, `url`, `status`, `size`) VALUES ' % self.tableName
			sql += ",".join(query)
			sql += ';'
			print sql
	
	def pushData(self, sql):
		mycursor = self.mydb.cursor()
		try:
			mycursor.execute("CREATE DATABASE IF NOT EXISTS %s;" % self.database)
			self.getDatabaseLink();
			mycursor = self.mydb.cursor()
			mycursor.execute("CREATE TABLE `%s` ( `id` int(11) NOT NULL, `ip` varchar(16) DEFAULT NULL, `request_date` timestamp NULL DEFAULT NULL, `type` varchar(10) NOT NULL,`url` text,`status` int(10) NOT NULL,`size` varchar(10) NOT NULL) ENGINE=MyISAM DEFAULT CHARSET=latin1;" % self.tableName)
			mycursor.execute("ALTER TABLE `%s` ADD PRIMARY KEY (`id`);" % self.tableName)
			mycursor.execute("ALTER TABLE `%s` MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;" % self.tableName)
			mycursor.execute(sql)
		except Error as err:
			print err

if __name__ == "__main__":
	if not len(sys.argv) > 1:
		print (__doc__)
		sys.exit(1)
	accessLogPath 	= sys.argv[1]
	tableName    	= sys.argv[2]
	
	try:
		if len(sys.argv)==7:
			host   		 	= sys.argv[3]
			database    	= sys.argv[4]
			user    		= sys.argv[5]
			password    	= sys.argv[6]
			log = Logs(accessLogPath, tableName, host, database, user, password)
		else:
			log = Logs(accessLogPath, tableName)
			
	except IOError:
		print ("You must specify a valid file to parse")
		print (__doc__)
		sys.exit(1)
    
	