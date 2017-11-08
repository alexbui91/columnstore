/*
 * Dictionary.h
 *
 *  Created on: Oct 6, 2017
 *      Author: alexbui
 */

#ifndef SRC_DICTIONARY_H_
#define SRC_DICTIONARY_H_


#include "ColumnBase.h"
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <iostream>

using namespace std;

template<class T>
class Dictionary {
private:
	struct invertedIndex {
		string word;
		char position;	 // position on text
		vector<size_t> location; // position on dictionary
		// functions
		bool operator<(const invertedIndex& a) const
		{
			return word < a.word;
		}
		bool operator==(const invertedIndex& a) const
		{
			return word == a.word;
		}
	};
	// vector to save all unique items of each column
	vector<T>* items;
	bool isSorted = false;
	std::unordered_map<T, size_t>* sMap;
	vector<T>* bulkVecValue;
	vector<invertedIndex>* vecIndexLevel0;
public:
	Dictionary() {
		items = new vector<T>();
		sMap = new unordered_map<T, size_t>();
		bulkVecValue = new vector<T>();
		vecIndexLevel0 = new vector<invertedIndex>();
	}
	virtual ~Dictionary() {
		delete items;
		delete vecIndexLevel0;
		delete sMap;
		delete bulkVecValue;
	}

	T* lookup(size_t index);
	vector<T>* getItems(){
		return items;
	}
	size_t addItem(T& value, vector<size_t>* vecValue, bool sorted, bool bulkInsert);
	size_t size();
	void setIsSorted(bool sorted);
	bool getIsSorted();
	void print(int row);
	void search(ColumnBase::OP_TYPE opType, vector<size_t>& result, T& value);
	void search(ColumnBase::OP_TYPE opType, vector<size_t>& result, T& value, T& value2);
	vector<T>* get_all();
	void sort();
	vector<T>* getBulkVecValue() {
		return bulkVecValue;
	}
	void clearTemp() {
		sMap->clear();
		bulkVecValue->resize(0);
	}
	int getMemoryConsumption(){
		return sizeof(items) + (sizeof(T) * this->size());
	}
	void test(){
		cout << "test success";
	}

};



#endif /* SRC_DICTIONARY_H_ */
