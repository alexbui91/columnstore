/*
 * ColumnBase.h
 *
 *  Created on: Oct 6, 2017
 *      Author: alexbui
 */

#ifndef SRC_COLUMNBASE_H_
#define SRC_COLUMNBASE_H_

#include <string>

namespace std {

class ColumnBase {
public:
	enum COLUMN_TYPE {uIntType, intType, llType, charType, varcharType};
	enum OP_TYPE {equal, ne, lt, le, gt, ge, range, rangeEQl, rangeEQr, rangeEQ};
private:
	string name;
	COLUMN_TYPE type;
public:
	// constructor
	ColumnBase();

	string getName();
	void setName(string nameValue);
	COLUMN_TYPE getType();
	void setType(COLUMN_TYPE typeValue);
	static bool is_contain_op(OP_TYPE op){
		if(op >= OP_TYPE::equal && op <= OP_TYPE::rangeEQ){
			return true;
		}
		return false;
	}
};

} /* namespace std */

#endif /* SRC_COLUMNBASE_H_ */
