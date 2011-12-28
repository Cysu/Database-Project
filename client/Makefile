CC		= g++
FLAGS	= -O2 -c -D_FILE_OFFSET_BITS=64 -I/usr/local/include -L/usr/local/lib
LINKS	= -lkyotocabinet -lz -lstdc++ -lrt -lpthread -lm -lc
OBJS	= $(patsubst %.cpp, %.o, $(wildcard *.cpp))

all : $(OBJS)

%.o : %.cpp
	$(CC) $(FLAGS) -o $@ $< $(LINKS)

.PHONY : clean

clean :
	rm -f *.o

