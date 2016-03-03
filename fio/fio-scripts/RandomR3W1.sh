set +x
testName="RandomR3W1"
for grid in {2..11}
do
	mkdir /grid/$grid/fio-$testName
        printf "==========================================\n"
        echo "Grid No: = $grid"    
        bsArray=(4k 8k 32k 64k 128k 256k 1M 5M)
        for bsize in "${bsArray[@]}"
        do
                printf "_____________________________________________________________\n"
                echo "Block size =$bsize";      
                
                fileName=/grid/$grid/fio-$testName/test-$bsize
                touch $fileName
		echo "File Generated =$fileName"; 
                start_time=`date +%s`   
		fio --filename=$fileName --randrepeat=1 --ioengine=sync --direct=1 --gtod_reduce=1 --iodepth=64 --readwrite=randrw --rwmixread=75 --name RandomR3W1 --sync=1 --bs=$bsize  --numjobs=`nproc` --time_based --runtime=30 --group_reporting --norandommap --size=4G
                end_time=`date +%s`;
                echo Time taken `expr $end_time - $start_time` s;
        done;
done;
