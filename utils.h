/*
 * utils.h
 *
 *  Created on: Oct 26, 2017
 *      Author: alex
 */

#ifndef UTILS_H_
#define UTILS_H_

#include <string>
#include <algorithm>
#include <cstring>
#include <sys/time.h>

using namespace std;

using bigint = long long int;

namespace utils {
inline void removeCharsFromString(string &str, char* charsToRemove) {
	for (unsigned int i = 0; i < strlen(charsToRemove); ++i) {
		str.erase(remove(str.begin(), str.end(), charsToRemove[i]), str.end());
	}
}

inline long get_timestamp() {
	struct timeval tp;
	gettimeofday(&tp, NULL);
	long ms = tp.tv_sec * 1000 + tp.tv_usec / 1000;
	return ms;
}
}
;
#endif /* UTILS_H_ */
