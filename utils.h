/*
 * utils.h
 *
 *  Created on: Oct 26, 2017
 *      Author: alex
 */

#ifndef UTILS_H_
#define UTILS_H_

#include <string>
#include "ColumnBase.h";
#include "Column.h";

using namespace std;

using bigint = long long int;

namespace utils {
void removeCharsFromString(string &str, char* charsToRemove) {
	for (unsigned int i = 0; i < strlen(charsToRemove); ++i) {
		str.erase(remove(str.begin(), str.end(), charsToRemove[i]), str.end());
	}
}

template<class T>
Column<T>* get_column(vector<ColumnBase*>* columns, vector<ColumnBase::COLUMN_TYPE>* col_type, int i){
	switch (col_type->at(i)) {
	case ColumnBase::uIntType: {
		Column<unsigned int>* col =
				(Column<unsigned int>*) columns->at(i);
		return col;
	}
	case ColumnBase::intType: {
		Column<int>* col = (Column<int>*) columns->at(i);
		return col;
	}
	case ColumnBase::llType: {
		Column<bigint>* col = (Column<bigint>*) columns->at(i);
		return col;
	}
	default:
		Column<string>* col = (Column<string>*) columns->at(i);
		return col;
	}
}


}
;

#endif /* UTILS_H_ */
