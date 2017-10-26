/*
 * utils.h
 *
 *  Created on: Oct 26, 2017
 *      Author: alex
 */

#ifndef UTILS_H_
#define UTILS_H_

#include <string>

using namespace std;

namespace utils{
	void removeCharsFromString( string &str, char* charsToRemove ) {
	   for ( unsigned int i = 0; i < strlen(charsToRemove); ++i ) {
	      str.erase( remove(str.begin(), str.end(), charsToRemove[i]), str.end() );
	   }
	}
};


#endif /* UTILS_H_ */
