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
	enum COLUMN_TYPE {uIntType, intType, doubleType, charType, varcharType};
	enum OP_TYPE {equalOp, neOp, ltOp, leOp, gtOp, geOp, containOp};
private:
	string name;
	COLUMN_TYPE type;
public:
	// constructor
	ColumnBase();

	string getName();
	void setName(string nameValue);
	COLUMN_TYPE getType();
	void setType(ColumnBase::COLUMN_TYPE typeValue);
};

} /* namespace std */

#endif /* SRC_COLUMNBASE_H_ */
