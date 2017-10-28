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

#include "ColumnBase.h"
#include "Column.h"

#include "utils.h"

using namespace std;

using bigint = long long int;

typedef boost::bimap<size_t, unsigned int> pos_id;
typedef boost::bimap<size_t, size_t> map_idx_idx;
typedef pos_id::value_type position;
typedef map_idx_idx::value_type idx2pos;

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

	pos_id select_all(int i, map<size_t, size_t>& row_dict, vector<size_t>* rowids = NULL) {
		Column<unsigned int>* rid = (Column<unsigned int>*) this->columns->at(
				i);
		rowids = new vector<size_t>();
		size_t index = -1;
		// first dimension translation table
		unsigned int value = 0U;
		pos_id tmp_dict;
		pos_id::left_const_iterator it;
		for(size_t i = 0; i < this->length; i++){
			rowids->push_back(i);
			// position in dictionary
			index = rid->lookup_packed(i);
			// actual value
			row_dict[i] = *(*rid).getDictionary()->lookup(index);
			it = tmp_dict.left.find(index);
			if (it == tmp_dict.left.end()) {
				value = row_dict[i];
				if(value != NULL){
					tmp_dict.insert(position(index, value));
				}
			}
		}
		return tmp_dict;
	}
	// input is a list of postion in dictionary
	// need to look up rowid in vecValue
	// c, col is selection column
	// i, rid is id column or join column
	// map_idx_idx is a map its key is row id and value is dictionary pos
	// return a map of <dict_pos, dict_actual_value>
	pos_id lookup_id(vector<size_t>& input, int c, int i, map<size_t, size_t>& row_dict, vector<size_t>* rowids = NULL) {
		Column<unsigned int>* rid = (Column<unsigned int>*) this->columns->at(
				i);
		Column<unsigned int>* col = (Column<unsigned int>*) this->columns->at(
				c);
		// row_ids of selection results
		if(rowids == NULL){
			rowids = col->lookup_rowid(length, input);
		}
		cout << "length of rowids " << rowids -> size() << endl;
		// create map of dict_pos vs dict_actual_value
//		map<size_t, unsigned int>* tmp_dict = new map<size_t, unsigned int>();
		pos_id tmp_dict;
		pos_id::left_const_iterator it;
		size_t index = 0;
		size_t row_id = -1;
		unsigned int value = 0U;
		for(size_t i = 0; i < (*rowids).size(); i++){
			row_id = rowids->at(i);
			// index in dictionary
			index = rid->lookup_packed(row_id);
			// actual value
			row_dict[row_id] = *(*rid).getDictionary()->lookup(index);
			it = tmp_dict.left.find(index);
			if (it == tmp_dict.left.end()) {
				value = row_dict[row_id];
				if(value != NULL){
					tmp_dict.insert(position(index, value));
				}
			}
//			if(tmp_dict->find(index) == tmp_dict->end()){
//				(*tmp_dict)[index] = *rid->getDictionary()->lookup(index);
//			}
		}
//		cout << "size of dictionary " << tmp_dict->size();
		return tmp_dict;
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
