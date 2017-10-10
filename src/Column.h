/*
 * Column.h
 *
 *  Created on: Oct 6, 2017
 *      Author: alexbui
 */

#ifndef SRC_COLUMN_H_
#define SRC_COLUMN_H_

#include "ColumnBase.h"
#include "PackedArray.h"
#include "Dictionary.h"

namespace std {
template<typename T>
class Column: public ColumnBase{
private:
	// value vector for column
	vector<size_t>* vecValue;
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
	}
	vector<size_t>* getVecValue(){
//		if (vecValue == NULL) {
//			vecValue = new vector<size_t>();
//		}
//		vecValue->clear();
//		for (int i = 0; i < packed->count; i++) {
//			vecValue->push_back(PackedArray_get(packed, i));
//		}
		return vecValue;
	}
	PackedArray* getPacked(){
		return packed;
	}
	Dictionary<T>* getDictionary(){
		return dictionary;
	}
	void updateDictionary(T &value, bool sorted = false, bool bulkInsert = true){
		this->bulkInsert = bulkInsert;
		dictionary->addItem(value, vecValue, sorted, bulkInsert);
	}
};

}



#endif /* SRC_COLUMN_H_ */
