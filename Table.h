/*
 * Table.h
 *
 *  Created on: Oct 24, 2017
 *      Author: alex
 */

#ifndef TABLE_H_
#define TABLE_H_

#include <string>

#include <boost/bimap.hpp>

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
	size_t length;

public:
	Table(string tname, vector<ColumnBase::COLUMN_TYPE>* ctype,
			vector<string>* cname) {
		name = tname;
		col_type = ctype;
		col_name = cname;
		columns = new vector<ColumnBase*>();
		length = 0;
	}
	Table() {
		name = "";
		col_type = new vector<ColumnBase::COLUMN_TYPE>();
		col_name = new vector<string>();
		columns = new vector<ColumnBase*>();
		length = 0;
	}
	virtual ~Table() {
		delete col_type;
		delete col_name;
		delete columns;
	}
	string getName() {
		return name;
	}
	vector<ColumnBase*>* getColumns() {
		return columns;
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
		string name;
		ifstream infile(path);
		if (!infile) {
			cout << "Cannot open file: " << path << endl;
		} else {
			for (size_t i = 0; i != col_type->size(); i++) {

				ColumnBase* col_b = new ColumnBase();
				name = col_name->at(i);
				switch (col_type->at(i)) {
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
				col_b->setType(col_type->at(i));
				this->columns->push_back(col_b);
			}
			// init ts lossy col
			Column<long>* ts_lossy = new Column<long>();
			// load data into corresponding column
			size_t last = 0;
			size_t next = 0;
			string temp_col;
			bool flag = false;
			size_t row = 0;
			vector<string> col_str;
			while (getline(infile, line)) {
				last = 0;
				next = 0;
				// count total column
				flag = false;
				col_str.clear();
				while ((next = line.find(delim, last)) != string::npos) {
					temp_col = line.substr(last, next - last);
					last = next + delim.length();
					col_str.push_back(temp_col);
				}
				temp_col = line.substr(last);
				col_str.push_back(temp_col);
				for (last = 0; last < col_str.size(); last++) {
					temp_col = col_str.at(last);
					ColumnBase* col_b = columns->at(last);
					flag = true;
					switch (col_b->getType()) {
					case ColumnBase::uIntType: {
						Column<unsigned int>* col =
								(Column<unsigned int>*) col_b;
						unsigned int cell = 0;
						try {
							cell = (unsigned int) stoi(temp_col);
						} catch (int e) {
							cout << "can't convert value " << temp_col
									<< " to integer";
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
							cout << "can't convert value " << temp_col
									<< " to integer";
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
							cout << "can't convert value " << temp_col
									<< " to big int";
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
				}

				if (flag) {
					row++;
				}
			}
			length = row;
		}
		infile.close();
//		processColumn();
	}
	void processColumn(bool is_lossy = false) {
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

	vector<unsigned int>* select_all(int i) {
		vector<unsigned int>* result;
		Column<unsigned int>* col = (Column<unsigned int>*) this->columns->at(
				i);
		result = col->getDictionary()->get_all();
		return result;
	}

	// input is a list of postion in dictionary
	// need to look up rowid in vecValue
	// c, col is selection column
	// i, rid is id column or join column
	void lookup_id(vector<size_t>& input, int c, int i) {
		Column<unsigned int>* rid = (Column<unsigned int>*) this->columns->at(
				i);
		Column<unsigned int>* col = (Column<unsigned int>*) this->columns->at(
				c);
		cout << "length of input" << input.size() << endl;
		vector<size_t>* rowid = col->lookup_rowid(length, input);
//		unordered_map<size_t, unsigned int>* tmp_dict = new unordered_map<size_t, unsigned int>();
		boost::bimap<size_t, unsigned int>* tmp_dict;
		size_t index = 0;
		for(size_t i = 0; i < (*rowid).size(); i++){
			index = rowid->at(i);
			if(tmp_dict->find(index) == tmp_dict->end()){
				(*tmp_dict).insert(bm_type::value_type(index, *rid->getDictionary()->lookup(index)));
			}
		}
		cout << "size of dictionary " << tmp_dict->size();
		// get dictionary positions
//		for(size_t i = 0; i < rowid->size(); i++){
//			cout << rowid->at(i) << endl;
//		}
	}
	void print_table(int limit) {
		cout << "Print table data " << this->getName() << endl;
		if (this->is_table_exist()) {
			string tmp = "";
//			ColumnBase* base = this->columns->at(0);
//			Column<unsigned int>* col = (Column<unsigned int>*) base;
//			cout << col->getName() << endl;
//			for(size_t j = 0; j < length; j++){
//				cout << col->getVecValue()->at(j) << endl;
//			}
			for(size_t j = 0; j < length; j++){
				tmp = "|";
				for(size_t i = 0; i < this->columns->size(); i++){
					switch (col_type->at(i)) {
					case ColumnBase::uIntType: {
						Column<unsigned int>* col =
								(Column<unsigned int>*) this->columns->at(i);
						tmp.append(to_string(col->getVecValue()->at(j))).append("|");
						break;
					}
					case ColumnBase::intType: {
						Column<int>* col = (Column<int>*) this->columns->at(i);
						tmp.append(to_string(col->getVecValue()->at(j))).append("|");
						break;
					}
					case ColumnBase::llType: {
						Column<bigint>* col = (Column<bigint>*) this->columns->at(i);
						tmp.append(to_string(col->getVecValue()->at(j))).append("|");
						break;
					}
					default:
						Column<string>* col = (Column<string>*) this->columns->at(i);
						tmp.append(to_string(col->getVecValue()->at(j))).append("|");
						break;
					}
				}
				cout << tmp << endl;
				tmp.clear();
			}
		}
	}
	bool is_table_exist() {
		return this->columns->size() != 0;
	}
	long to_lossy(string temp_col) {
		return stol(temp_col.substr(0, temp_col.length() / 2));
	}
};

#endif /* TABLE_H_ */
