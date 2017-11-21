/*
 * Dictionary.cpp
 *
 *  Created on: Oct 6, 2017
 *      Author: alexbui
 */

#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <iostream>
#include <sstream>
#include <iterator>
#include <map>
#include <unordered_map>
#include <algorithm>
#include <locale>
#include "Dictionary.h"

using namespace std;

using bigint = long long int;

template<class T>
bool smaller(T value1, T value2) {
	return value1 < value2;
};

template<class T>
bool equal(T value1, T value2) {
	return value1 == value2;
};

bool equal(string value1, int value2) {
	return false;
};

template<class T>
void Dictionary<T>::setIsSorted(bool sorted) {
	this->isSorted = sorted;
}

template<class T>
bool Dictionary<T>::getIsSorted() {
	return this->isSorted;
}

template<class T>
T* Dictionary<T>::lookup(size_t index) {
	if (items->empty() || index < 0 || index >= items->size()) {
		return NULL;
	} else {
		return &items->at(index);
	}
}
template<class T>
size_t Dictionary<T>::addItem(T& value, vector<size_t>* vecValue, bool sorted, bool bulkInsert) {
	// bulk insert
	if (bulkInsert) bulkVecValue->push_back(value);

	if (items->empty()) {
		items->push_back(value);
		vecValue->push_back(0);
		(*sMap)[value] = 1;
		return 0;
	} else if (!sorted) {
		// normal case, bulk insert
		// check if value existed on dictionary
		if ((*sMap)[value] == 0) {
			items->push_back(value);
			vecValue->push_back(items->size() - 1);
			(*sMap)[value] = vecValue->back() + 1;
		}
		else {
			// get position from temp map
			vecValue->push_back((*sMap)[value] - 1);
		}
		return vecValue->back();
	} else {
		// find the lower bound for value in vector
		typename vector<T>::iterator lower;
		lower = std::lower_bound(items->begin(), items->end(), value,
				equal<T>);
		// value existed
		if (lower != items->end() && equal(value, *lower)) {
			// return the position of lower
			long elementPos = lower - items->begin();
			vecValue->push_back(elementPos);
			return elementPos;
		} else {
			// The position of new element in dictionary
			size_t newElementPos = 0L;
			if (lower == items->end()) {
				// insert to the end of dictionary
				newElementPos = items->size();
				items->push_back(value);
				vecValue->push_back(newElementPos);
			} else {
				newElementPos = lower - items->begin();
				// insert into dictionary
				items->insert(lower, value);
				// update (+1) to all elements in vecValue have value >= newElementPos
				if (!bulkInsert) {
					for (int i = 0; i < vecValue->size(); i++) {
						if (vecValue->at(i) >= newElementPos) {
							++vecValue->at(i);
						}
					}
				}
				vecValue->push_back(newElementPos);
			}

			// return the position of new element
			return newElementPos;
		}
	}
}

template<class T>
void Dictionary<T>::search(ColumnBase::OP_TYPE opType, vector<size_t>& result, T& value) {
//	cout << items->size() << endl;;
	if (items->empty()) {
		// result is empty
		result.push_back(-1);
	} else if(isSorted) {
		// search with sorted option
		// find the lower bound for value in vector
		typename vector<T>::iterator lower;
		lower = std::lower_bound(items->begin(), items->end(), value,
				smaller<T>);

		// based on operator to find exact position in dictionary
		switch (opType) {
		case ColumnBase::equal: {
			size_t sz = items->size();
			if (lower != items->end() && equal(*lower, value)) {
				// return position in dictionary
				result.push_back(lower - items->begin());
			} else {
				// result is empty
				result.push_back(-1);
			}
			break;
		}
		case ColumnBase::ne: {
			int exclusivePosition = -1;
			if (lower != items->end() && equal(*lower, value)) {
				exclusivePosition = lower - items->begin();
			}
			// return all dictionary positions except exclusiveValue
			for (size_t i = 0; i < items->size(); i++) {
				if (i != exclusivePosition) {
					result.push_back(i);
				}
			}
			break;
		}
		case ColumnBase::lt: {
			// less than
			// return positions from 0 to lower
			for (size_t i = 0;
					(lower == items->end()) ?
							i < items->size() : i < (lower - items->begin());
					i++) {
				result.push_back(i);
			}
			break;
		}
		case ColumnBase::le: {
			// less than and equal to
			unsigned int position = -1;
			if (lower == items->end()) {
				// all are smaller than
				position = items->size();
			} else if (equal(*lower, value)) {
				position = (lower - items->begin()) + 1;
			} else {
				position = lower - items->begin();
			}
			// return from 0 to position
			for (size_t i = 0; i < position; i++) {
				result.push_back(i);
			}
			break;
		}
		case ColumnBase::gt: {
			unsigned int position = items->size();
			if (lower == items->end()) {
				// all items are less than value return null
				position = items->size();
			} else if (equal(*lower, value)) {
				position = (lower - items->begin()) + 1;
			} else {
				// similar to ge if not contain equal value
				position = lower - items->begin();
			}
			// return from postion to items.size()
			for (size_t i = position; i < items->size(); i++) {
				result.push_back(i);
			}
			break;
		}
		case ColumnBase::ge: {
			// return from lower to items.size()
			unsigned int i =
					(lower == items->end()) ?
							items->size() : (lower - items->begin());
			for (; i < items->size(); i++) {
				result.push_back(i);
			}
			break;
		}
		}
	}
	else {
		// Search with non-sorted dic => scan through all items
		for (size_t i = 0; i < items->size(); i++) {
			T dictionaryValue = items->at(i);
			// based on operator to find exact position in dictionary
			switch (opType) {
			case ColumnBase::equal: {
				if (equal(dictionaryValue, value)) {
					result.push_back(i);
					return;
				}
				break;
			}
			case ColumnBase::ne: {
				// not equal
				if (!equal(value, dictionaryValue)) {
					result.push_back(i);
				}
				break;
			}
			case ColumnBase::lt: {
				// less than
				if (smaller(dictionaryValue, value)){
					result.push_back(i);
				}
				break;
			}
			case ColumnBase::le: {
				// less than or equal = not greater than
				if (!smaller(value, dictionaryValue)) {
					result.push_back(i);
				}
				break;
			}
			case ColumnBase::gt: {
				// greater than
				if (smaller(value, dictionaryValue)){
					result.push_back(i);
				}
				break;
			}
			case ColumnBase::ge: {
				// greater than or equal = not less than
				if (!smaller(dictionaryValue, value)) {
					result.push_back(i);
				}
				break;
			}
			}
		}
	}
}

template<class T>
void Dictionary<T>::search(ColumnBase::OP_TYPE opType, vector<size_t>& result, T& value, T& value2) {
	size_t length = size();
	if (items->empty()) {
		// result is empty
		result.push_back(-1);
	} else if(isSorted) {
		// search with sorted option
		// find the lower bound for value in vector
		typename vector<T>::iterator lower;
		typename vector<T>::iterator upper;
		lower = std::lower_bound(items->begin(), items->end(), value,
				smaller<T>);
		upper = std::lower_bound(items->begin(), items->end(), value2,
						smaller<T>);
		// based on operator to find exact position in dictionary
		switch (opType) {
		case ColumnBase::range: {
			// less than value2, greater than value 1
			unsigned int position = -1;
			if (lower == items->end()) {
				// all items are less than value
				position = length;
			} else if (equal(*lower, value)) {
				position = (lower - items->begin()) + 1;
			} else {
				position = lower - items->begin();
			}
			// from position "lower bound"
			// to position either end or (upper - begin)
			for (size_t i = position; (upper == items->end()) ?
					i < length : i < (upper - items->begin()); i++) {
				result.push_back(i);
			}
			break;
		}
		case ColumnBase::rangeEQ: {
			unsigned int up = -1;

			// less than & eq value 2
			if (upper == items->end()) {
				// all are smaller than
				up = length;
			} else if (equal(*upper, value2)) {
				up = (upper - items->begin()) + 1;
			} else {
				up = upper - items->begin();
			}
			// greater & eq value 1
			unsigned int i = (lower == items->end()) ? length : (lower - items->begin());
			// from position "lower bound i"
			// to upper bound
			for (; i < up; i++) {
				result.push_back(i);
			}
			break;
		}
		case ColumnBase::rangeEQr: {
			unsigned int up = -1;
			unsigned int lp = -1;

			// less than & eq value 2
			if (upper == items->end()) {
				// all are smaller than
				up = length;
			} else if (equal(*upper, value2)) {
				up = (upper - items->begin()) + 1;
			} else {
				up = upper - items->begin();
			}
			// greater than value 1
			if (lower == items->end()) {
				lp = length;
			} else if (equal(*lower, value)) {
				lp = (lower - items->begin()) + 1;
			} else {
				lp = lower - items->begin();
			}
			for (size_t i = lp; i < up; i++) {
				result.push_back(i);
			}
			break;
		}
		case ColumnBase::rangeEQl: {
			unsigned int size = length;

			// less than value 2
			unsigned int up = (upper == items->end()) ? size : (upper - items->begin());
			// greater & eq value 1
			unsigned int i = (lower == items->end()) ? size : (lower - items->begin());
			// from position "lower bound i"
			// to upper bound
			for (; i < up; i++) {
				result.push_back(i);
			}
			break;
		}
		}
	}
	else {
		// Search with non-sorted dic => scan through all items
		switch(opType){
		case ColumnBase::range:{
			for (size_t i = 0; i < length; i++) {
				T dictionaryValue = items->at(i);
				if (smaller(value, dictionaryValue) && smaller(dictionaryValue, value2)){
					result.push_back(i);
				}
			}
			break;
		}
		case ColumnBase::rangeEQ:{
			for (size_t i = 0; i < length; i++) {
				T dictionaryValue = items->at(i);
				if (!smaller(dictionaryValue, value) && !smaller(value2, dictionaryValue)){
					result.push_back(i);
				}
			}
			break;
		}
		case ColumnBase::rangeEQl:{
			for (size_t i = 0; i < length; i++) {
				T dictionaryValue = items->at(i);
				if (!smaller(dictionaryValue, value) && smaller(dictionaryValue, value2)){
					result.push_back(i);
				}
			}
			break;
		}
		case ColumnBase::rangeEQr:{
			for (size_t i = 0; i < length; i++) {
				T dictionaryValue = items->at(i);
				if (smaller(value, dictionaryValue) && !smaller(value2, dictionaryValue)){
					result.push_back(i);
				}
			}
			break;
		}
		}
	}
}
template<class T>
vector<T>* Dictionary<T>::get_all(){
	return items;
}

template<class T>
void Dictionary<T>::sort() {
	std::sort(items->begin(), items->end(), smaller<T>);
}

template<class T>
size_t Dictionary<T>::size() {
	return items->size();
}

template<class T>
void Dictionary<T>::print(int row) {
	cout << "Dictionary[" << row << "] = " << items->at(row) << "\n";
}

template class Dictionary<string>;
template class Dictionary<bigint>;
template class Dictionary<unsigned int>;
template class Dictionary<int>;
template class Dictionary<long>;
