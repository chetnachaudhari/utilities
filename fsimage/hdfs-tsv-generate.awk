BEGIN {
	blocksize=134217728;
	replicationFile=3;
	replicationDir=0;
	accessTime="1970-01-01 05:30";
	numBlockDir="-1";
	fileSizeDir=0;
	namespaceQuotaDir=0;
	diskSpaceQuotaDir=-1;
	print "/" "\t" 0 "\t" "2016-02-02 15:56" "\t" "1970-01-01 05:30" "\t" "0" "\t"	"-1" "\t"	"0"	"\t" "9223372036854775807" "\t"	"-1" "\t" "rwxrwxr-x" "\t" 	"hdfs" "\t" "hadoop";
}
{
	path=$8;
	perms=$1;
	username=$3;
	groupname=$4;
	fileSize=$5;

	if( $2 == "-"){ 
		print $8 "\t" 0 "\t" $6 " " $7 "\t" accessTime "\t" "0" "\t" "-1" "\t"  "0" "\t" "-1" "\t" "-1" "\t" perms "\t" username "\t" groupname;
	} else {
		numBlocks=int(fileSize/blocksize) + 1;
		print $8 "\t" $2 "\t" $6 " " $7 "\t" accessTime "\t" blocksize "\t" numBlocks "\t" fileSize  "\t" perms "\t" username "\t" groupname;	}
}
END {
	
}