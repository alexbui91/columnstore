/*
 * Column.h
 *
 *  Created on: Oct 6, 2017
 *      Author: alexbui
 */

#ifndef SRC_COLUMN_H_
#define SRC_COLUMN_H_

#include <iostream>
#include <vector>
#include <thread>
#include <mutex>
#include <chrono>
#include <map>
#include <set>
#include "ColumnBase.h"
#include "Dictionary.h"
#include "PackedArray.h"
#include "utils.h"

using namespace std;

template<typename T>
class Column: public ColumnBase {
private:
	// value vector for column corresponding (whole column with each row id) to position in items of dictionary
	vector<size_t>* vecValue;
	vector<size_t>* lookup_result;
	// bit packing array
	PackedArray* packed;
	// dictionary vector for column
	Dictionary<T>* dictionary;
	// versions space of each col is a map of
	// row_id -> map_to_vx
	// csn(tx) -> value
	map<long, map<size_t, long>*> *versions;

	map<long, set<size_t>*>* working_transactions;

	bool bulkInsert = false;

public:
	Column() {
		dictionary = new Dictionary<T>();
		vecValue = new vector<size_t>();
		packed = new PackedArray();
		lookup_result = new vector<size_t>();
		versions = new map<long, map<size_t, long>*>();
		working_transactions = new map<long, set<size_t>*>();
	}

	virtual ~Column() {
		delete vecValue;
		delete dictionary;
		delete lookup_result;
		delete versions;
		delete working_transactions;
		PackedArray_destroy(packed);
	}
	vector<size_t>* getVecValue() {
		// change to bit type
		if (vecValue == NULL) {
			vecValue = new vector<size_t>();
		}
		if (packed->count) {
			vecValue->clear();
			for (size_t i = 0; i < packed->count; i++) {
				vecValue->push_back(PackedArray_get(packed, i));
			}
		}
		return vecValue;
	}
	PackedArray* getPacked() {
		return packed;
	}
	Dictionary<T>* getDictionary() {
		if (dictionary == NULL) {
			dictionary = new Dictionary<T>();
		}
		return dictionary;
	}
//	size_t get_total_versions(){
//
//	}
	void updateDictionary(T& value, bool sorted = false,
			bool bulkInsert = true) {
		this->bulkInsert = bulkInsert;
		dictionary->addItem(value, vecValue, sorted, bulkInsert);
	}
	//  re-order vecValue after building entire dictionary using bulk insert
	void rebuildVecValue() {
		if (this->bulkInsert) {
			vecValue->resize(0);
			// sort dictionary
			dictionary->sort();
			dictionary->setIsSorted(true);
			// get bulkVecValue vector
			vector<T>* bulkVecValue = dictionary->getBulkVecValue();
			if (bulkVecValue != NULL) {
				for (size_t i = 0; i < bulkVecValue->size(); i++) {
					// find position of valueId in dictionary
					vector<size_t> result;
					dictionary->search(ColumnBase::equal, result,
							bulkVecValue->at(i));
					long pos = result[0];
					if (pos != -1)
						vecValue->push_back(pos);
				}
			}
			bulkVecValue->resize(0);
		}
	}

	void processColumn() {
		if (this->getType() == ColumnBase::intType
				|| this->getType() == ColumnBase::uIntType
				|| this->getType() == ColumnBase::llType) {
			this->rebuildVecValue();
			this->createBitPackingVecValue();
			this->getDictionary()->clearTemp();
		}
	}
	void createBitPackingVecValue() {
		size_t numOfBit = (size_t) ceil(log2((double) dictionary->size()));
		// init bit packing array
		packed = PackedArray_create(numOfBit, vecValue->size());

		for (size_t i = 0; i < vecValue->size(); i++) {
			size_t value = vecValue->at(i);
			// value is position in dict
			// i is position in original column
			PackedArray_set(packed, i, value);
		}
		// free space vecValue
		vecValue->resize(0);
	}
	// selection with version space
	bool selection(T& searchValue, ColumnBase::OP_TYPE q_where_op,
			vector<bool>* q_resultRid, size_t &tx, bool initQueryResult = false) {
		vector<size_t> result;
		this->getDictionary()->search(q_where_op, result, searchValue);
		// find rowId with appropriate dictionary position
		long dictPosition;
		for (size_t rowId = 0; !result.empty() && rowId < packed->count;
				rowId++) {
			// before lookup dataspace, lookup in version space
			dictPosition = scan_version(rowId, tx);
			if (dictPosition == -1) {
				dictPosition = this->lookup_packed(rowId);
			}
			if ((ColumnBase::is_contain_op(q_where_op)
					&& (size_t)dictPosition >= result.front()
					&& (size_t)dictPosition <= result.back())) {
				// first where expr => used to init query result
				if (initQueryResult)
					q_resultRid->push_back(true); //rowId is in query result
				else {
					if (!q_resultRid->at(rowId)) {
						q_resultRid->at(rowId) = true;
					}
				}
			} else {
				// rowId is not in query result
				if (initQueryResult)
					q_resultRid->push_back(false);
				else
					q_resultRid->at(rowId) = false;
			}
		}
		return true;
	}

	bool selection(T& searchValue, ColumnBase::OP_TYPE q_where_op,
			vector<bool>* q_resultRid, bool initQueryResult = false) {
		vector<size_t> result;
		this->getDictionary()->search(q_where_op, result, searchValue);
		// find rowId with appropriate dictionary position
		size_t dictPosition;
		for (size_t rowId = 0; !result.empty() && rowId < packed->count;
				rowId++) {
			dictPosition = this->lookup_packed(rowId);
			if ((ColumnBase::is_contain_op(q_where_op)
					&& dictPosition >= result.front()
					&& dictPosition <= result.back())) {
				// first where expr => used to init query result
				if (initQueryResult)
					q_resultRid->push_back(true); //rowId is in query result
				else {
					if (!q_resultRid->at(rowId)) {
						q_resultRid->at(rowId) = true;
					}
				}
			} else {
				// rowId is not in query result
				if (initQueryResult)
					q_resultRid->push_back(false);
				else
					q_resultRid->at(rowId) = false;
			}
		}
		return true;
	}

	vector<T> projection(vector<bool>* q_resultRid, size_t limit,
			size_t& limitCount) {
		vector<T> outputs; // output result
		limitCount = 0; // reset limit count
		size_t encodeValue;
		for (size_t rid = 0; rid < q_resultRid->size(); rid++) {
			if (q_resultRid->at(rid)) {
				encodeValue = this->lookup_packed(rid);
				T* a = this->getDictionary()->lookup(encodeValue);
				outputs.push_back(*a);
				if (++limitCount >= limit)
					break;
			}
		}

		return outputs;
	}

	vector<T> projection(vector<int>* q_resultRid, size_t limit,
			size_t& limitCount) {
		vector<T> outputs; // output result
		limitCount = 0; // reset limit count
		for (size_t i = 0; i < q_resultRid->size(); i++) {
			size_t encodeValue = this->lookup_result(q_resultRid->at(i));
			T* a = this->getDictionary()->lookup(encodeValue);
			outputs.push_back(*a);
			if (++limitCount >= limit)
				break;
		}

		return outputs;
	}

	// look up row_id that fits to condition_result
	void lookup_rowid(size_t length, vector<size_t>& lookup_result,
			vector<size_t> *rowids) {
		size_t pos = -1;
//		clock_t t1;
		for (size_t i = 0; i < length; i++) {
//			t1 = clock();
			pos = lookup_packed(i);
			if (this->getDictionary()->getIsSorted()) {
				if (pos != -1
						&& binary_search(lookup_result.begin(),
								lookup_result.end(), pos)) {
					rowids->push_back(i);
				}
			} else if (pos != -1
					&& find(lookup_result.begin(), lookup_result.end(), pos)
							!= lookup_result.end()) {
				rowids->push_back(i);
			}
//			cout << "one step cost: " << (float) clock() - (float) t1 << endl;
//			break;
		}
	}

	// try master and slave
	void lookup_rowid_master(size_t length, vector<size_t>& lookup_result,
			vector<size_t> *rowids) {
		int no_of_slave = 4;
		mutex mtx;
		size_t length_of_slave = length / no_of_slave;
		size_t from = 0;
		size_t to = 0;
		vector<thread> list_thread;
		bool isSorted = this->getDictionary()->getIsSorted();

		vector<size_t> pos1;
		from = 0;
		to = length_of_slave;
		for (size_t i = from; i < to; i++) {
			pos1.push_back(this->lookup_packed(i));
		}
		vector<size_t> tmp_id1;
		list_thread.push_back(
				thread(&Column<T>::lookup_rowid_slave, ref(mtx), ref(pos1),
						isSorted, ref(lookup_result), ref(tmp_id1), from, to));
		from = to;
		to = length_of_slave + from;
		vector<size_t> pos2;
		for (size_t i = from; i < to; i++) {
			pos2.push_back(this->lookup_packed(i));
		}
		vector<size_t> tmp_id2;
		list_thread.push_back(
				thread(&Column<T>::lookup_rowid_slave, ref(mtx), ref(pos2),
						isSorted, ref(lookup_result), ref(tmp_id2), from, to));
		from = to;
		to = length_of_slave + from;
		vector<size_t> pos3;
		for (size_t i = from; i < to; i++) {
			pos3.push_back(this->lookup_packed(i));
		}
		vector<size_t> tmp_id3;
		vector<size_t> pos4;
		list_thread.push_back(
				thread(&Column<T>::lookup_rowid_slave, ref(mtx), ref(pos3),
						isSorted, ref(lookup_result), ref(tmp_id3), from, to));
		from = to;
		to = length;
		for (size_t i = from; i < to; i++) {
			pos4.push_back(this->lookup_packed(i));
		}
		vector<size_t> tmp_id4;
		list_thread.push_back(
				thread(&Column<T>::lookup_rowid_slave, ref(mtx), ref(pos4),
						isSorted, ref(lookup_result), ref(tmp_id4), from, to));

		for (int i = 0; i < no_of_slave; i++) {
			list_thread.at(i).join();
		}
//		cout << "l: " << tmp_id1.size() << "||" << tmp_id2.size() << "||" << tmp_id3.size() << "||" << tmp_id4.size() << "||" << endl;
		rowids->insert(rowids->end(), tmp_id1.begin(), tmp_id1.end());
		rowids->insert(rowids->end(), tmp_id2.begin(), tmp_id2.end());
		rowids->insert(rowids->end(), tmp_id3.begin(), tmp_id3.end());
		rowids->insert(rowids->end(), tmp_id4.begin(), tmp_id4.end());
	}

	static void lookup_rowid_slave(mutex& mtx, vector<size_t>& pos,
			bool isSorted, vector<size_t>& lookup_result,
			vector<size_t> &rowids, size_t from, size_t to) {
		bool flag = false;
		long p = -1;
		cout << pos.size() << endl;
		size_t length = to - from;
		for (size_t i = 0; i < length; i++) {
			p = pos.at(i);
			flag = false;
			if (isSorted) {
				if (p != -1
						&& binary_search(lookup_result.begin(),
								lookup_result.end(), p)) {
					flag = true;
				}
			} else if (p != -1
					&& find(lookup_result.begin(), lookup_result.end(), p)
							!= lookup_result.end()) {
				flag = true;
			}
			if (flag) {
				mtx.lock();
				rowids.push_back(from + i);
				mtx.unlock();
			}
		}
	}

	// return -1 in case of missing
	size_t lookup_packed(size_t i) {
		size_t pos = -1;
		if (packed->count) {
			pos = PackedArray_get(packed, i);
		} else {
			pos = this->vecValue->at(i);
		}
		return pos;
	}
//
	void printVecValue(int row) {
		for (size_t i = 0; i < (*vecValue).size() && i < row; i++) {
			cout << "vecValue[" << i << "] = " << (*vecValue)[i] << "\n";
		}
	}

	// implement versions space
	// check if version already has key row_id
	map<size_t, long>* is_version_exist(size_t row_id) {
		try{
			map<long, map<size_t, long>*>::iterator it;
			it = versions->find(row_id);
			if (it != versions->end()) {
				return it->second;
			}
		}catch (exception& e) {
			cerr << "Error during check version of row: " << row_id << " Exception: " << e.what() << endl;
		}
		return NULL;
	}

	// insert new version of rowid to spaceb
	void create_version(size_t& tx, size_t& row_id, T& value) {
		vector<size_t> results;
//		cout << "col: " << this->getName() << this->getDictionary()->getItems()->size() << endl;
		this->getDictionary()->search(ColumnBase::OP_TYPE::equal, results, value);
		if (results.size()) {
			size_t dict_position = results.at(0);
			if((long)dict_position != -1){
				create_version(tx, row_id, dict_position);
			}else{
//				cout << value << " not in dictionary of" << this->getName();
			}
		}
	}
	// insert new version of rowid with dict_position to space
	void create_version(size_t& tx, size_t& row_id, size_t& dict_position) {
		try{
			map<size_t, long>* s = is_version_exist(row_id);
			if (s == NULL) {
				s = new map<size_t, long>();
				s->insert(pair<size_t, long>(tx, dict_position));
				versions->insert(pair<long, map<size_t, long>*>(row_id, s));
			}else{
				s->insert(pair<size_t, long>(tx, dict_position));
			}
		}catch (exception& e) {
			cerr << "Error during create version of row: " << row_id << " and dict_pos: " << dict_position << " - Exception: " << e.what() << endl;
		}
	}

	// implement versions space
	// check if version already has key row_id
	set<size_t>* is_transaction_working(size_t row_id) {
		try{
			map<long, set<size_t>*>::iterator it;
			it = working_transactions->find(row_id);
			if (it != working_transactions->end()) {
				return it->second;
			}
		}catch (exception& e) {
			cerr << "Error during validate transaction, Exception: " << e.what() << endl;
		}
		return NULL;
	}
	T lookup_dictionary(size_t& tx, size_t row_id){
		size_t dict = scan_version(row_id, tx);
		T* a = this->getDictionary()->lookup(dict);
		return *a;
	}
	// scan for row id value with timestamp value
	long scan_version(size_t& row_id, size_t& tx) {
		long value = -1;
		map<size_t, long>* s = is_version_exist(row_id);
		map<size_t, long>::iterator it;
		if (s != NULL) {
//			it = (*s).lower_bound(tx);
			// upper_bound: find first element that larger than tx
			// given is the previous one of this element
			// if iterator pointer is at the first position mean nothing found
			it = s->upper_bound(tx);
			if (it != s->begin()) {
				it = prev(it, 1);
				value = it->second;
				// add to working transaction on current row_id of version control
				set<size_t>* trans = is_transaction_working(row_id);
				if (trans == NULL) {
					trans = new set<size_t>();
					trans->insert(tx);
					working_transactions->insert(
							pair<long, set<size_t>*>(row_id, trans));
				} else {
					auto i = trans->find(tx);
					if (i == trans->end()) {
						trans->insert(tx);
					}
				}
			}
		}
		return value;
	}

	// record version outdated => remove
	int collect_garbage(size_t row_id) {
		int flag = 0;
		set<size_t>* trans = is_transaction_working(row_id);
		map<size_t, long>* s = is_version_exist(row_id);
		map<size_t, long>::iterator it;
		try{
			if (trans != NULL) {
				if (trans->empty()) {
					working_transactions->erase(row_id);
					if (s != NULL) {
						flag += s->size();
						versions->erase(row_id);
					}
				} else {
					set<size_t>::iterator iter = trans->begin();
					if (iter != trans->end()) {
						size_t pos = distance(trans->begin(), iter);
						size_t min_tx = *iter;
						// find tx in working transaction of row_id
						if (s != NULL) {
							it = s->upper_bound(min_tx);
							if(it != s->begin()){
								s->erase(s->begin(), it);
								flag = pos + 1;
							}
						}
					}
				}
			}else{
				if (s != NULL) {
					flag += s->size();
					versions->erase(row_id);
				}
			}
		}catch(exception& e){
			cerr <<  "Error during collect garbage transaction, Exception: " << e.what() << endl;
		}
		return flag;
	}
	// remove transation working from row_id map
	void deactivate_transaction(size_t tx, size_t row_id){
		set<size_t>* trans = is_transaction_working(row_id);
		if(trans != NULL){
			try{
			if (trans->empty()) {
				working_transactions->erase(row_id);
			}else{
				set<size_t>::iterator it;
				it = trans->find(tx);
				if(it != trans->end()){
					trans->erase(it);
				}
			}}catch(exception& e){
				cerr <<  "Error during exit transaction, Exception: " << e.what() << endl;
			}
		}

	}

	// calling update all latest versions
	// may used annually
	long update_latest_version() {
		long total = 0;
		size_t row_id;
		if(!versions->empty()){
			map<long, map<size_t, long>*>::iterator it;
			map<size_t, long>* rows;
			for(it = versions->begin(); it != versions->end(); it++){
				rows = it->second;
				row_id = it->first;
				if (!rows->empty()) {
					update_latest_version(row_id, rows);
					total += collect_garbage(row_id);
				}
			}
		}
//		cout << "FFFFFF" << total << endl;
		return total;
	}
	// get latest version of row_id then update to data space
	void update_latest_version(size_t row_id) {
		map<size_t, long>* s = is_version_exist(row_id);
		if (s != NULL && !s->empty()) {
			update_latest_version(row_id, s);
		}
	}
	void update_latest_version(size_t row_id, map<size_t, long>* vs) {
		map<size_t, long>::reverse_iterator it;
		long value = -1;
		try{
			if(!vs->empty()){
				it = vs->rbegin();
				if (it != vs->rend()) {
					value = it->second;
					if(value != -1){
						PackedArray_set(packed, row_id, value);
					}else{
//						cout << "FFF " << value << endl;
					}
				}
			}
		}catch(exception& e){
			string mes = "Error during update table space " + to_string(row_id) + "|";
			if(value != -1){
				mes += to_string(value);
			}
			cerr <<  mes << ", Exception: " << e.what() << endl;
		}

	}
};

#endif /* SRC_COLUMN_H_ */
