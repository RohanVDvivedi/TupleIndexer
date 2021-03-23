
gcc -Wall ./test_sorted_packed_page.c -o test_spp.out -lroti -lstupstom -lbufferpool -lrwlock -lcutlery && valgrind -v ./test_spp.out

#gcc -Wall ./test.c -o test.out -lroti -lstupstom -lbufferpool -lrwlock -lcutlery && valgrind -v ./test.out