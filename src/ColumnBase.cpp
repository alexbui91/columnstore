/*
 * ColumnBase.cpp
 *
 *  Created on: Oct 6, 2017
 *      Author: alexbui
 */

#include "ColumnBase.h"

namespace std {
	ColumnBase::ColumnBase(){
		name = "";
	}
	string ColumnBase::getName(){
		return name;
	}
	ColumnBase::COLUMN_TYPE ColumnBase::getType(){
		return type;
	}
	void ColumnBase::setName(string localName){
		name = localName;
	}
	void ColumnBase::setType(ColumnBase::COLUMN_TYPE localType){
		type = localType;
	}
}

