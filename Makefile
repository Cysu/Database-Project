CC		= g++
FLAGS	= -O2 -D_FILE_OFFSET_BITS=64

.PHONY : all clean

all :
	$(MAKE) --directory=client
	$(MAKE) --directory=lib
	$(CC) $(FLAGS) -o run main.cpp client/*.o lib/*.o

clean :
	$(MAKE) --directory=client clean
	$(MAKE) --directory=lib clean
	rm -f run data/*


