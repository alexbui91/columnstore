/*
 * Column.h
 *
 *  Created on: Oct 6, 2017
 *      Author: alexbui
 */

#ifndef SRC_COLUMN_H_
#define SRC_COLUMN_H_

#include "ColumnBase.h"
#include "Dictionary.h"
#include "PackedArray.h"

namespace std {
template<typename T>
class Column: public ColumnBase{
private:
	// value vector for column corresponding (whole column with each row id) to position in items of dictionary
	vector<size_t>* vecValue;
	// bit packing array
//	PackedArray* packed;
	// dictionary vector for column
	Dictionary<T>* dictionary;

	bool bulkInsert = false;

public:
	Column() {
		dictionary = new Dictionary<T>();
		vecValue = new vector<size_t>();
//		packed = new PackedArray();
	}

	virtual ~Column() {
		delete vecValue;
		delete dictionary;
//		PackedArray_destroy(packed);
	}
	vector<size_t>* getVecValue(){
		// change to bit type
//		if (vecValue == NULL) {
//			vecValue = new vector<size_t>();
//		}
//		vecValue->clear();
//		for (int i = 0; i < packed->count; i++) {
//			vecValue->push_back(PackedArray_get(packed, i));
//		}
		return vecValue;
	}
//	PackedArray* getPacked(){
//		return packed;
//	}
	Dictionary<T>* getDictionary(){
		if (dictionary == NULL) {
			dictionary = new Dictionary<T>();
		}
		return dictionary;
	}
	void updateDictionary(T& value, bool sorted = false, bool bulkInsert = true){
		this->bulkInsert = bulkInsert;
		dictionary->addItem(value, vecValue, sorted, bulkInsert);
	}

	//  re-order vecValue after building entire dictionary using bulk insert
	void rebuildVecValue() {
		if(this->bulkInsert){
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
					dictionary->search(ColumnBase::equal, result, bulkVecValue->at(i));
					size_t pos = result[0];
					if (pos != -1) vecValue->push_back(pos);
				}
			}
			bulkVecValue->resize(0);
		}
	}

	void processColumn() {
		if (this->getType() == ColumnBase::intType ||
				this->getType() == ColumnBase::uIntType ||
				this->getType() == ColumnBase::llType) {
			this->rebuildVecValue();
//			this->createBitPackingVecValue();
			this->getDictionary()->clearTemp();
		}
	}
//	void createBitPackingVecValue() {
//		size_t numOfBit = (size_t) ceil(log2((double) dictionary->size()));
//		// init bit packing array
//		packed = PackedArray_create(numOfBit, vecValue->size());
//
//		for (size_t i = 0; i < vecValue->size(); i++) {
//			size_t value = vecValue->at(i);
//			// value is position in dict
//			// i is position in original column
//			PackedArray_set(packed, i, value);
//		}
//		// free space vecValue
//		vecValue->resize(0);
//	}
//
//	size_t lookup_packed(size_t i){
//		return PackedArray_get(packed, i);
//	}
//
//	void printVecValue(int row) {
//		vecValue = getVecValue();
//		for (size_t i = 0; i < (*vecValue).size() && i < row; i++) {
//			cout << "vecValue[" << i << "] = " << (*vecValue)[i] << "\n";
//		}
//	}
};

}



#endif /* SRC_COLUMN_H_ */
