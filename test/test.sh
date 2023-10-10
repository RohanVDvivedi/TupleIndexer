gcc -Wall ./test_$1.c -o test_$1.out -ltupleindexer -ltuplestore -lrwlock -lcutlery
if [ $? -eq 0 ]
then
	if [ "$2" = "vald" ]
	then
		sudo valgrind -v --track-origins=yes ./test_$1.out
	elif [ "$2" = "time" ]
	then
		sudo time -v ./test_$1.out
	else
		./test_$1.out > $2
		echo "output saved to $2"
	fi
fi
