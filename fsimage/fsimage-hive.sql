CREATE TABLE INODE_DETAILS (
         ID INT,
         TYPE VARCHAR(20),
         NAME VARCHAR(1000),
         REPLICATION INT,
         MTIME VARCHAR(25),
         ATIME VARCHAR(25),
         BLOCKSIZE INT,
         NUMBER_BLOCKS INT,
         FILESIZE BIGINT,
         NSQUOTA BIGINT,
         DSQUOTA BIGINT,
         USERNAME VARCHAR(100),
         GROUPNAME VARCHAR(100),
         PERMISSION VARCHAR(25))
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS ORC;


CREATE TABLE INODE (
         ID INT,
         PARENT INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS ORC;

CREATE TABLE INODE_PATH (
         ID INT,
         PATH VARCHAR(10000))
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS ORC;


LOAD DATA LOCAL INPATH '/tmp/hdfsdu/idAndPathNew.csv' INTO TABLE INODE_PATH;
LOAD DATA LOCAL INPATH '/tmp/hdfsdu/fsimage-inodes-dir-map-new-filter.csv' INTO TABLE INODE;
LOAD DATA LOCAL INPATH '/tmp/hdfsdu/fsimage-inodes-complete-formatted.csv' INTO TABLE INODE_DETAILS;

create table stats as select a.id, b.path, a.type, a.mtime, a.atime, a.replication, a.blocksize, a.number_blocks, a.filesize, a.nsquota, a.dsquota, a.username, a.groupname, a.permission from inode_details a join inode_path b on a.id = b.id;