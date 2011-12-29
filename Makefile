CC		= g++
FLAGS	= -O2 -D_FILE_OFFSET_BITS=64 -I/usr/local/include -L/usr/local/lib
LINKS	= -lkyotocabinet -lz -lstdc++ -lrt -lpthread -lm -lc

.PHONY : all clean

all :
	$(MAKE) --directory=client
	$(MAKE) --directory=lib
	$(CC) $(FLAGS) -o run main.cpp client/*.o lib/*.o $(LINKS)

clean :
	$(MAKE) --directory=client clean
	$(MAKE) --directory=lib clean
	rm -f data/*

clear:
	rm -f data/*

