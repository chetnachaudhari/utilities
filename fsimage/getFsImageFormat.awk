BEGIN {
	FS=","
}
{	
	if( $2 == " DIRECTORY" ) {
		split($5,permissions,":")
		print $1 "\t" $2 "\t" $3 "\t" "0" "\t" $4 "\t" $4 "\t" "0" "\t" "-1" "\t"  "0" "\t" $6 "\t" $7 "\t" permissions[3] "\t" permissions[1] "\t" permissions[2];
	} else if( $2 == " FILE" ) {
		split($8,permissions,":")
		count=0;
		filesize=0;
		for(i=11;i<=NF;){
			count=count+1;
			if($i != "-")
				filesize=filesize+$i;
			i=i+4
		};
		print $1 "\t" $2 "\t" $3 "\t" $4 "\t" $5 "\t" $6 "\t" $7 "\t" count "\t"  filesize "\t" "-1" "\t" "-1" "\t" permissions[3] "\t" permissions[1] "\t" permissions[2];
	}
}
END {
	
}
