/*
 * Expr.cpp
 *
 *  Created on: Nov 8, 2017
 *      Author: alex
 */

#ifndef EXPR_CPP_
#define EXPR_CPP_

#include "ColumnBase.h";

using namespace std;

class Expr{
private:
	string val;
	string field;
	ColumnBase::OP_TYPE op;
public:
	Expr(string &nval, string &nfield, ColumnBase::OP_TYPE &nop){
		val = nval;
		field = nfield;
		op = nop;
	}
	virtual ~Expr() {
	}

	string getVal(){
		return val;
	}
	void setVal(string& nval){
		val = nval;
	}
	string getField(){
		return field;
	}
	void setField(string& nfield){
		field = nfield;
	}
	ColumnBase::OP_TYPE getOp(){
		return op;
	}
	void setOp(ColumnBase::OP_TYPE nop){
		op = nop;
	}
};


#endif /* EXPR_CPP_ */
