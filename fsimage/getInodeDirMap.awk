function ltrim(s) { sub(/^[ \t\r\n]+/, "", s); return s }
function rtrim(s) { sub(/[ \t\r\n]+$/, "", s); return s }
function trim(s) { return rtrim(ltrim(s)); }
BEGIN {
	FS=","
}
{	
	for(i=2;i<=NF;i++){
		if( $i != " ")
			print trim($i) "\t" trim($1);
	}
}	
END {
	
}
