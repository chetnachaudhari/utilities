#!/usr/bin/python

from xml.dom.minidom import parse
import xml.dom.minidom
import MySQLdb as mdb
import sys

try:
   con = mdb.connect('10.32.165.187', 'irm_rw', 'fdpirm', 'irm');

   cur = con.cursor()

   # Create table as per requirement
   inode_details_sql = """CREATE TABLE INODE_DETAILS (
         ID INT PRIMARY KEY,
         TYPE CHAR(20) NOT NULL,
         NAME VARCHAR(1000),
         MTIME VARCHAR(25),
         ATIME VARCHAR(25),
         USERNAME VARCHAR(100),
         GROUPNAME VARCHAR(100),
         PERMISSION VARCHAR(25),
         NSQUOTA BIGINT,
         DSQUOTA BIGINT,
         REPLICATION INT,
         BLOCKSIZE INT,
         NUMBER_BLOCKS INT,
         FILESIZE BIGINT"""

   cur.execute(inode_details_sql)

   inode_sql = """CREATE TABLE INODE (
         ID INT PRIMARY KEY,
         PARENT INT"""

   cur.execute(inode_sql)

   inode_path_sql = """CREATE TABLE INODE_PATH (
         ID INT PRIMARY KEY,
         PATH VARCHAR(10000)"""

   cur.execute(inode_path_sql)
   cur.commit()
except mdb.Error, e:
  
   print "Error %d: %s" % (e.args[0],e.args[1])
   sys.exit(1)
    
# print "Database version : %s " % ver
# Open XML document using minidom parser
DOMTree = xml.dom.minidom.parse("/grid/3/fsimage.xml")
fsimage = DOMTree.documentElement

inodeSection = fsimage.getElementsByTagName("INodeSection")

inodeDirectorySection = fsimage.getElementsByTagName("INodeDirectorySection")

inodes = inodeSection.getElementsByTagName("inode")

for inode in inodes:
   id = inode.getElementsByTagName('id')
   idValue = id.childNodes[0].data
   type = inode.getElementsByTagName('type')
   typeValue = type.childNodes[0].data
   name = inode.getElementsByTagName('name')
   nameValue = name.childNodes[0].data
   mtime = inode.getElementsByTagName('mtime')
   mtimeValue = mtime.childNodes[0].data
   permission = inode.getElementsByTagName('permission')
   permissionValue = permission.childNodes[0].data
   nsquota = inode.getElementsByTagName('nsquota')
   nsquotaValue = nsquota.childNodes[0].data
   dsquota = inode.getElementsByTagName('dsquota')
   dsquotaValue = dsquota.childNodes[0].data
   permSplits = permissionValue.split(':')
   userValue = permSplits[0]
   groupValue = permSplits[1]
   permValue = permSplits[2]
   
   if(typeValue == "DIRECTORY"):
      replicationValue = 0
      preferredBlockSizeValue = 0
      filesize = 0
      no_blocks = -1
      atimeValue = mtimeValue
   if(typeValue == "FILE"):   
      replication = inode.getElementsByTagName('replication')
      replicationValue = replication.childNodes[0].data
      atime = inode.getElementsByTagName('atime')
      atimeValue = atime.childNodes[0].data
      preferredBlockSize = inode.getElementsByTagName('perferredBlockSize')
      preferredBlockSizeValue = preferredBlockSize.childNodes[0].data
      blocks = inode.getElementsByTagName('blocks')
      no_blocks = 0
      filesize = 0
      for block in blocks:
         no_blocks = no_blocks + 1
         numBytes = block.getElementsByTagName('numBytes')
         filesizeValue = numBytes.childNodes[0].data
         filesize = filesize + filesizeValue

   try:
      sql = "INSERT INTO INODE_DETAILS(ID, TYPE, NAME, MTIME, ATIME, USERNAME, GROUPNAME, PERMISSION, NSQUOTA, DSQUOTA, REPLICATION, BLOCKSIZE, NUMBER_BLOCKS, FILESIZE) \
       VALUES ('%d', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%d', '%d', '%d', '%d', '%d', '%d' )" % \
       (idValue, typeValue, nameValue, mtimeValue, atimeValue, userValue, groupValue, permValue, nsquotaValue, dsquotaValue, replicationValue, preferredBlockSizeValue, no_blocks, filesize)         

      cur.execute(sql)
      con.commit()
   except:
      con.rollback()   

directories = inodeDirectorySection.getElementsByTagName("directory")
for directory in directories:
   parent = directory.getElementsByTagName('parent')
   parentValue = parent.childNodes[0].data
   children = directory.getElementsByTagName('inode')
   for child in children:
      childId = child.childNodes[0].data
      try:
         sql = "INSERT INTO INODE(ID, PARENT) VALUES ('%d', '%d')" % (childId, parentValue)
         cur.execute(sql)
         con.commit()
      except:
         con.rollback() 

if con:    
   con.close()
