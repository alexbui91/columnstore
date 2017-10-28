CXXFLAGS =	-O2 -g -Wall -fmessage-length=0 -std=c++11 -lstdc++ -lsqlparser -L/usr/local/lib/ -I/home/alex/workspacec/sql-parser/src/

OBJS =		main.o Table.o  Dictionary.o Column.o ColumnBase.o PackedArray.o utils.o 

LIBS =		-L/usr/local/lib/ -lsqlparser

TARGET =	main

$(TARGET):	$(OBJS)
	$(CXX) -o $(TARGET) $(OBJS) $(LIBS)

all:	$(TARGET)

clean:
	rm -f $(OBJS) $(TARGET)
