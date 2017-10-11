#include <iostream>
#include <fstream>
#include <string>
#include <algorithm>

#include "Dictionary.h"
#include "Column.h"
#include "ColumnBase.h"

using namespace std;

template <class T>
bool contain(vector<T> &vec, T &item){
	if(std::find(vec.begin(), vec.end(), item) != vec.end())
		return true;
	else
		return false;
}

using bigint = long long int;

int main(void){
	string path = "/home/alex/Documents/database/assignment2/raw/sample-game.csv";
	ifstream infile(path);
	if (!infile) {
		cout << "Cannot open file: " << path << endl;
		return -1;
	}
	string line;
	string delim = ",";
	vector<size_t> col_p = {0, 1, 5};
	vector<string> col_name = {"sid", "ts", "v"};
	vector<ColumnBase::COLUMN_TYPE> col_type = {ColumnBase::uIntType, ColumnBase::llType, ColumnBase::uIntType};
	vector<ColumnBase*> columns;
	string name;
	size_t t_index = 0;
	for (auto i = col_p.begin(); i != col_p.end(); i++){
		ColumnBase* col_b = new ColumnBase();
		name = col_name.at(t_index);
		switch(col_type.at(t_index)){
			case ColumnBase::uIntType:{
				Column<unsigned int>* col = new Column<unsigned int>();
				col_b = col;
				break;
			}
			case ColumnBase::intType:{
				Column<int>* col2 = new Column<int>();
				col_b = col2;
				break;
			}
			case ColumnBase::llType:{
				Column<bigint>* col3 = new Column<bigint>();
				col_b = col3;
				break;
			}
			default:
				Column<string>* col4 = new Column<string>();
				col_b = col4;
				break;
		}
		col_b->setName(name);
		col_b->setType(col_type.at(t_index));
		columns.push_back(col_b);
		t_index++;
	}
	// load data into corresponding column
	while(getline(infile, line)) {
		size_t last = 0; size_t next = 0;
		// count total column
		size_t c_index = 0;
		// count selected column
		t_index = 0;
		while((next = line.find(delim, last)) != string::npos){
			if(contain(col_p, c_index)){
				string temp_col = line.substr(last, next - last);
				ColumnBase* col_b = columns.at(t_index);
				switch(col_b->getType()){
					case ColumnBase::uIntType:{
						Column<int>* col = (Column<int>*) col_b;
						int cell = stoi(temp_col);
						col->updateDictionary(cell);
						break;
					}
					case ColumnBase::intType:{
						Column<int>* col = (Column<int>*) col_b;
						int cell = stoi(temp_col);
						col->updateDictionary(cell);
						break;
					}
					case ColumnBase::llType:{
						Column<bigint>* col = (Column<bigint>*) col_b;
						bigint cell = stod(temp_col);
						col->updateDictionary(cell);
						break;
					}
					default:
						Column<string>* col = (Column<string>*) col_b;
						col->updateDictionary(temp_col);
						break;
				}
				t_index++;
			}
			last = next + delim.length();
			c_index++;
		}
	}
	infile.close();

//	// perform search SID = 40
//	vector<size_t> result;
//	ColumnBase* col_b = columns.at(0);
//	Column<unsigned int>* sid = (Column<unsigned int>*) col_b;
//	unsigned int input = 40U;
//	sid->getDictionary()->search(input, ColumnBase::equal, result);
//	// print result sid
//
//	// V > 5,000,000
//	result.clear();
//	ColumnBase* col_b = columns.at(2);
//	Column<unsigned int>* v = (Column<unsigned int>*) col_b;
//	input = 5000000U;
//	sid->getDictionary()->search(input, ColumnBase::lt, result);
//	return 0;
}
