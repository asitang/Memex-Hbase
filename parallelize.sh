export parser=/Users/asitangm/Desktop/parser-indexer
export infolder=/Users/asitangm/Desktop/
export outfolder=/Users/asitangm/Desktop/out/

cd $parser
#SECONDS=0
max=2

for i in `seq 1 $max`
do
	java -jar target/parser-indexer-1.0-SNAPSHOT.jar postdump -in $infolderin$i -out $outfodlerjson$i &
	#PID$i=$!
done

# for i in `seq 1 $max`
# do
# 	ps -p $PID$i
# done

#wait $PID1 $PID2

#echo $SECONDS