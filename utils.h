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

namespace utils {
void removeCharsFromString(string &str, char* charsToRemove) {
	for (unsigned int i = 0; i < strlen(charsToRemove); ++i) {
		str.erase(remove(str.begin(), str.end(), charsToRemove[i]), str.end());
	}
}
template <typename T>
int find_position(vector<T>& v, T& value) {
	vector<int>::iterator i = v.begin();
	int nPosition = -1;
	while (i != v.end()) {
		cout << *i << endl;
		++i;
	}
	i = find(v.begin(), v.end(), value);
	if (i != v.end()) {
		nPosition = distance(v.begin(), i);
	}
	return nPosition;
}
}
;

#endif /* UTILS_H_ */
