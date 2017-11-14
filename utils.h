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
//#include <sys/time.h>
#include <chrono>

using namespace std;

using bigint = long long int;

namespace utils {
inline void removeCharsFromString(string &str, char* charsToRemove) {
	for (unsigned int i = 0; i < strlen(charsToRemove); ++i) {
		str.erase(remove(str.begin(), str.end(), charsToRemove[i]), str.end());
	}
}
/*
 * https://stackoverflow.com/questions/27677964/print-current-system-time-in-nanoseconds-using-c-chrono
 * http://www.cplusplus.com/reference/chrono/time_point/time_since_epoch/
 * */

inline size_t get_timestamp() {
//	struct timeval tp;
//	gettimeofday(&tp, NULL);
//	long ms = tp.tv_sec * 1000 + tp.tv_usec;
	chrono::system_clock::time_point tp = chrono::system_clock::now();
	chrono::system_clock::duration dtn = tp.time_since_epoch();
	auto nanoseconds = chrono::duration_cast<chrono::nanoseconds>(dtn);
	size_t ms = nanoseconds.count();
	return ms;
}

inline bool start_with(string &s, string pat){
	return s.find(pat) == 0;
}
}
;
#endif /* UTILS_H_ */
