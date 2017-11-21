--------------------
How to build and run this code
--------------------
- Prepare to build:
  + gcc 5.4+
  + make sure your machine supports g++ 11
  # download latest hyrise/sql-parser: https://github.com/hyrise/sql-parser
  # follow installation instruction
  + unzip downloaded file
  $ cd sql-parser
  $ make
  $ make install // it may require root user to perform
  $ make test // to make sure everything go well
  
- Build: 
  $ unzip assignment2
  $ cd assignment2/Debug
  # before build, please edit data path in main.cpp to your local data file
  # path look like this: string path = "/home/alex/Documents/database/assignment2/raw/sample-game.csv";
  $ make clean
  $ make all
- Run (maybe use run file contained):
  $ ./assignment2 port_no
  
  
