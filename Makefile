CXXFLAGS = -O2 -g -Wall -fmessage-length=0 -std=c++11 -lstdc++ -L/usr/local/Cellar/boost/1.65.1/include -I/sql

OBJS =		main.o Table.o  Dictionary.o Column.o ColumnBase.o PackedArray.o utils.o 

LIBS =	-lsqlparser

TARGET =	main

$(TARGET):	$(OBJS)
	$(CXX) -o $(TARGET) $(OBJS) $(LIBS)

all:	$(TARGET)

clean:
	rm -f $(OBJS) $(TARGET)
