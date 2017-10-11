/*
 * Dictionary.cpp
 *
 *  Created on: Oct 6, 2017
 *      Author: alexbui
 */
#ifndef _Dictionary_
#define _Dictionary_

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
bool differ(T value1, T value2) {
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
//template<class T>
//size_t Dictionary<T>::addItem(T& value, vector<size_t>* vecValue, bool sorted, bool bulkInsert) {
//
//}

template<class T>
void Dictionary<T>::search(T& value, ColumnBase::OP_TYPE opType, vector<size_t>& result) {
	if (items->empty()) {
		// return -1 to show no result
		result.push_back(-1);
	} else if(isSorted) {
		// find the lower bound for value in vector
		typename vector<T>::iterator lower;
		lower = std::lower_bound(items->begin(), items->end(), value,
				differ<T>);

		// based on operator to find exact position in dictionary
		switch (opType) {
		case ColumnBase::equal: {
			if (lower != items->end() && equal(*lower, value)) {
				result.push_back(lower - items->begin());
			} else {
				// return -1 to show no result
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
			unsigned int position = -1;
			if (lower == items->end()) {
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
				// all items are less than value
				position = items->size();
			} else if (equal(*lower, value)) {
				position = (lower - items->begin()) + 1;
			} else {
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
		// Search with no sorted dictionary => scan through all items
		for (size_t i = 0; i < items->size(); i++) {
			T dictionaryValue = items->at(i);
			// based on operator to find exact position in dictionary
			switch (opType) {
			case ColumnBase::equal: {
				// equal
				if (equal(dictionaryValue, value)) {
					result.push_back(i);
					// return immediately because dictionary has no duplicate
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
			case ColumnBase::gt: {
							// greater than
							if (differ(value, dictionaryValue)){
								result.push_back(i);
							}
							break;
						}
						case ColumnBase::ge: {
							// greater than or equal = not less than
							if (!differ(dictionaryValue, value)) {
								result.push_back(i);
							}
							break;
						}
			case ColumnBase::lt: {
				// less than
				if (differ(dictionaryValue, value)){
					result.push_back(i);
				}
				break;
			}
			case ColumnBase::le: {
				// less than or equal = not greater than
				if (!differ(value, dictionaryValue)) {
					result.push_back(i);
				}
				break;
			}
			}
		}
	}
}


template class Dictionary<string>;
template class Dictionary<bigint>;
template class Dictionary<int> ;

#endif
