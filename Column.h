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
	void lookup_rowid_master(size_t length, vector<size_t>& lookup_result, vector<size_t> *rowids, int no_of_slave=4) {
		mutex mtx;
		size_t length_of_slave = length / no_of_slave;
		size_t from = 0;
		size_t to = 0;
		vector<thread> list_thread;
		bool isSorted = this->getDictionary()->getIsSorted();
		vector<size_t> pos;
		for(int i = 0; i < length; i++){
			pos.push_back(this->lookup_packed(i));
		}
		for(int i = 0; i < no_of_slave; i++){
			from = i * length_of_slave;
			if(i < (no_of_slave - 1)){
				to = from + length_of_slave;
			}else{
				to = length;
			}

			list_thread.push_back(thread(&Column<unsigned int>::lookup_rowid_slave, this, ref(mtx), ref(pos), ref(isSorted), ref(length), ref(lookup_result), ref(rowids), ref(from), ref(to), i));
		}
		for(int i = 0; i < no_of_slave; i++){
			list_thread.at(i).join();
		}
	}

	static void lookup_rowid_slave(mutex& mtx, vector<size_t>& pos, bool isSorted, size_t length, vector<size_t>& lookup_result, vector<size_t> *rowids, size_t from, size_t to){
		bool flag = false;
		size_t p = -1;
		for(size_t i = from; i < to; i++){
			p = pos.at(i);
			flag = false;
			if(isSorted){
				if(p != -1 && binary_search(lookup_result.begin(), lookup_result.end(), p)){
					flag = true;
				}
			}else if(p != -1 && find(lookup_result.begin(), lookup_result.end(), pos) != lookup_result.end()){
				flag = true;
			}
			if(flag){
				mtx.lock();
				rowids->push_back(i);
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
