/*
 * Table.h
 *
 *  Created on: Oct 24, 2017
 *      Author: alex
 */

#ifndef TABLE_H_
#define TABLE_H_

#include <string>
#include "ColumnBase.h";
#include "Column.h";

#include "utils.h";

using namespace std;

using bigint = long long int;

class Table {
private:
	string name;
	vector<ColumnBase::COLUMN_TYPE>* col_type;
	vector<string>* col_name;
	vector<ColumnBase*>* columns;

public:
	Table(string tname, vector<ColumnBase::COLUMN_TYPE>* ctype, vector<string>* cname) {
		name = tname;
		col_type = ctype;
		col_name = cname;
		columns = new vector<ColumnBase*>();
	}
	Table() {
		name = "";
		col_type = new vector<ColumnBase::COLUMN_TYPE>();
		col_name = new vector<string>();
		columns = new vector<ColumnBase*>();
	}
	virtual ~Table() {
		delete col_type;
		delete col_name;
		delete columns;
	}
	string getName() {
		return name;
	}
	void setName(string &nameValue) {
		name = nameValue;
	}
	//build structure without using lossy compression
	void build_structure(string path) {
		this->build_structure(path, false);
	}
	// build table structure from path file (csv) with column type and column name vector
	void build_structure(string path, bool is_lossy) {
		string line;
		string delim = ",";
		size_t t_index = 0;
		string name;
		ifstream infile(path);
		if (!infile) {
			cout << "Cannot open file: " << path << endl;
		} else {
			for (auto i = col_type->begin(); i != col_type->end(); i++) {

				ColumnBase* col_b = new ColumnBase();
				name = col_name->at(t_index);
				switch (col_type->at(t_index)) {
				case ColumnBase::uIntType: {
					Column<unsigned int>* col = new Column<unsigned int>();
					col_b = col;
					break;
				}
				case ColumnBase::intType: {
					Column<int>* col2 = new Column<int>();
					col_b = col2;
					break;
				}
				case ColumnBase::llType: {
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
				col_b->setType(col_type->at(t_index));
				this->columns->push_back(col_b);
				t_index++;
			}
			// init ts lossy col
			Column<long>* ts_lossy = new Column<long>();
			// load data into corresponding column
			size_t last = 0;
			size_t next = 0;
			string temp_col;
			ColumnBase* col_b;
			while (getline(infile, line)) {
				last = 0;
				next = 0;
				// count total column
				t_index = 0;
				while ((next = line.find(delim, last)) != string::npos) {
					temp_col = line.substr(last, next - last);
					last = next + delim.length();
					col_b = columns->at(t_index);
					switch (col_b->getType()) {
					case ColumnBase::uIntType: {
						Column<unsigned int>* col =
								(Column<unsigned int>*) col_b;
						unsigned int cell = 0;
						try {
							cell = (unsigned int) stoi(temp_col);
						} catch (int e) {
							cout << "can't convert value " << temp_col << " to integer";
						}
						col->updateDictionary(cell);
						break;
					}
					case ColumnBase::intType: {
						Column<int>* col = (Column<int>*) col_b;
						int cell = 0;
						try {
							cell = stoi(temp_col);
						} catch (int e) {
							cout << "can't convert value " << temp_col << " to integer";
						}
						col->updateDictionary(cell);
						break;
					}
					case ColumnBase::llType: {
						// TS
						Column<bigint>* col = (Column<bigint>*) col_b;
						bigint cell = 0ll;
						try {
							cell = stoll(temp_col);
						} catch (int e) {
							cout << "can't convert value " << temp_col << " to big int";
						}
						col->updateDictionary(cell);
						if (col->getName() == "ts") {
							long cell_lossy = to_lossy(temp_col);
							ts_lossy->updateDictionary(cell_lossy);
						}
						break;
					}
					default:
						Column<string>* col = (Column<string>*) col_b;
						utils::removeCharsFromString(temp_col, "\"");
						col->updateDictionary(temp_col);
						break;
					}
					t_index++;
				}
			}
		}
		infile.close();
//		processColumn(is_lossy);
	}
	void processColumn(bool is_lossy) {
		// update vector value using bulk insert
		Column<long>* ts_lossy = new Column<long>();
		for (size_t i = 0; i < this->columns->size(); i++) {
			switch (col_type->at(i)) {
			case ColumnBase::uIntType: {
				Column<unsigned int>* col =
						(Column<unsigned int>*) this->columns->at(i);
				col->processColumn();
				break;
			}
			case ColumnBase::intType: {
				Column<int>* col2 = (Column<int>*) this->columns->at(i);
				col2->processColumn();
				break;
			}
			case ColumnBase::llType: {
				Column<bigint>* col3 = (Column<bigint>*) this->columns->at(i);
				col3->processColumn();
				if (col3->getName() == "ts" && is_lossy) {
					ts_lossy->rebuildVecValue();
				}
				break;
			}
			default:
				Column<string>* col4 = (Column<string>*) this->columns->at(i);
				col4->processColumn();
				break;
			}
		}
	}

	void print_table(int limit){
		cout << "Print table data";
		if(this->is_table_exist()){
			cout << "Print table data 2";
			ColumnBase* base =  this->columns->at(0);
			Column<unsigned int>* id = (Column<unsigned int>*) base;
			vector<size_t>* value = id->getVecValue();
			string tmp = "";
			for(size_t j = 0; j < value->size(); j++){
				for(size_t i = 0; i < this->columns->size(); i++){
				}
				cout << tmp << endl;
				tmp.clear();
			}
		}
	}
	bool is_table_exist(){
		return this->columns->size() != 0;
	}
	long to_lossy(string temp_col) {
		return stol(temp_col.substr(0, temp_col.length() / 2));
	}
};

#endif /* TABLE_H_ */
