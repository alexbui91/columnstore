/*
 * Column.h
 *
 *  Created on: Oct 6, 2017
 *      Author: alexbui
 */

#ifndef SRC_COLUMN_H_
#define SRC_COLUMN_H_

#include<iostream>
#include<thread>
#include<mutex>
#include<chrono>

#include "ColumnBase.h"
#include "Dictionary.h"
#include "PackedArray.h"

namespace std {
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

	bool bulkInsert = false;

public:
	Column() {
		dictionary = new Dictionary<T>();
		vecValue = new vector<size_t>();
		packed = new PackedArray();
		lookup_result = new vector<size_t>();
	}

	virtual ~Column() {
		delete vecValue;
		delete dictionary;
		delete lookup_result;
		PackedArray_destroy(packed);
	}
	vector<size_t>* getVecValue() {
		// change to bit type
		if (vecValue == NULL) {
			vecValue = new vector<size_t>();
		}
		if(packed->count){
			vecValue->clear();
			for (int i = 0; i < packed->count; i++) {
				vecValue->push_back(PackedArray_get(packed, i));
			}
		}
		return vecValue;
	}
	PackedArray* getPacked(){
		return packed;
	}
	Dictionary<T>* getDictionary() {
		if (dictionary == NULL) {
			dictionary = new Dictionary<T>();
		}
		return dictionary;
	}
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
					size_t pos = result[0];
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

	bool selection(T& searchValue, ColumnBase::OP_TYPE q_where_op,
						vector<bool>* q_resultRid, bool initQueryResult = false) {
		vector<size_t> result;
		this->getDictionary()->search(q_where_op, result, searchValue);
		// find rowId with appropriate dictionary position
		for (size_t rowId = 0; !result.empty() && rowId < packed->count; rowId++) {
			size_t dictPosition = this->lookup_packed(rowId);
			if ((ColumnBase::is_contain_op(q_where_op) && dictPosition >= result.front() && dictPosition <= result.back())) {
				// first where expr => used to init query result
				if (initQueryResult)
					q_resultRid->push_back(true); //rowId is in query result
				else {
					if (!q_resultRid->at(rowId)) {
						q_resultRid->at(rowId) = true;
					}
				}
			}
			else {
				// rowId is not in query result
				if(initQueryResult)
					q_resultRid->push_back(false);
				else
					q_resultRid->at(rowId) = false;
			}
		}
		return true;
	}

	vector<T> projection(vector<bool>* q_resultRid, size_t limit, size_t& limitCount) {
		vector<T> outputs; // output result
		limitCount = 0; // reset limit count
		for (size_t rid = 0; rid < q_resultRid->size(); rid++) {
			if (q_resultRid->at(rid)) {
				size_t encodeValue = this->lookup_packed(rid);
				T* a = this->getDictionary()->lookup(encodeValue);
				outputs.push_back(*a);
				if (++limitCount >= limit) break;
			}
		}

		return outputs;
	}

	vector<T> projection(vector<int>* q_resultRid, size_t limit, size_t& limitCount) {
		vector<T> outputs; // output result
		limitCount = 0; // reset limit count
		for (size_t i = 0; i < q_resultRid->size(); i++) {
			size_t encodeValue = this->lookup_result(q_resultRid->at(i));
			T* a = this->getDictionary()->lookup(encodeValue);
			outputs.push_back(*a);
			if (++limitCount >= limit) break;
		}

		return outputs;
	}

	// look up row_id that fits to condition_result
	void lookup_rowid(size_t length, vector<size_t>& lookup_result, vector<size_t> *rowids) {
		size_t pos = -1;
//		clock_t t1;
		for(size_t i = 0; i < length; i++){
//			t1 = clock();
			pos = lookup_packed(i);
			if(this->getDictionary()->getIsSorted()){
				if(pos != -1 && binary_search(lookup_result.begin(), lookup_result.end(), pos)){
					rowids->push_back(i);
				}
			}else if(pos != -1 && find(lookup_result.begin(), lookup_result.end(), pos) != lookup_result.end()){
				rowids->push_back(i);
			}
//			cout << "one step cost: " << (float) clock() - (float) t1 << endl;
//			break;
		}
	}


	// try master and slave
	void lookup_rowid_master(size_t length, vector<size_t>& lookup_result, vector<size_t> *rowids) {
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
		for(int i = from; i < to; i++){
			pos1.push_back(this->lookup_packed(i));
		}
		vector<size_t> tmp_id1;
		list_thread.push_back(thread(&Column<T>::lookup_rowid_slave, ref(mtx), ref(pos1), isSorted, ref(lookup_result), ref(tmp_id1), from, to));
		from = to;
		to = length_of_slave + from;
		vector<size_t> pos2;
		for(int i = from; i < to; i++){
		    pos2.push_back(this->lookup_packed(i));
		}
		vector<size_t> tmp_id2;
		list_thread.push_back(thread(&Column<T>::lookup_rowid_slave, ref(mtx), ref(pos2), isSorted, ref(lookup_result), ref(tmp_id2), from, to));
		from = to;
		to = length_of_slave + from;
		vector<size_t> pos3;
		for(int i = from; i < to; i++){
		    pos3.push_back(this->lookup_packed(i));
		}
		vector<size_t> tmp_id3;
		vector<size_t> pos4;
		list_thread.push_back(thread(&Column<T>::lookup_rowid_slave, ref(mtx), ref(pos3), isSorted, ref(lookup_result), ref(tmp_id3), from, to));
		from = to;
		to = length;
		for(int i = from; i < to; i++){
		    pos4.push_back(this->lookup_packed(i));
		}
		vector<size_t> tmp_id4;
		list_thread.push_back(thread(&Column<T>::lookup_rowid_slave, ref(mtx), ref(pos4), isSorted, ref(lookup_result), ref(tmp_id4), from, to));

		for(int i = 0; i < no_of_slave; i++){
			list_thread.at(i).join();
		}
//		cout << "l: " << tmp_id1.size() << "||" << tmp_id2.size() << "||" << tmp_id3.size() << "||" << tmp_id4.size() << "||" << endl;
		rowids->insert(rowids->end(), tmp_id1.begin(), tmp_id1.end());
		rowids->insert(rowids->end(), tmp_id2.begin(), tmp_id2.end());
		rowids->insert(rowids->end(), tmp_id3.begin(), tmp_id3.end());
		rowids->insert(rowids->end(), tmp_id4.begin(), tmp_id4.end());
	}

	static void lookup_rowid_slave(mutex& mtx, vector<size_t>& pos, bool isSorted, vector<size_t>& lookup_result, vector<size_t> &rowids, size_t from, size_t to){
		bool flag = false;
		size_t p = -1;
		cout << pos.size() << endl;
		size_t length = to - from;
		for(size_t i = 0; i < length; i++){
			p = pos.at(i);
			flag = false;
			if(isSorted){
				if(p != -1 && binary_search(lookup_result.begin(), lookup_result.end(), p)){
					flag = true;
				}
			}else if(p != -1 && find(lookup_result.begin(), lookup_result.end(), p) != lookup_result.end()){
				flag = true;
			}
			if(flag){
				mtx.lock();
				rowids.push_back(from + i);
				mtx.unlock();
			}
		}
	}

	// return -1 in case of missing
	size_t lookup_packed(size_t i) {
		size_t pos = -1;
		if(packed->count){
			pos = PackedArray_get(packed, i);
		}else{
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

	// look up real value from dictionary items: find in A (1, apple_in_a)

	// look up position from real value from dictionary items: find in B (apple, 2_in_b)

	// mapping in vecValue (row_id of B) with dict position of column A (pos_a, row_id_b)
};

}

#endif /* SRC_COLUMN_H_ */
