#include <iostream>
#include <fstream>
#include <string>
#include <algorithm>
#include <ctime>

#include "stdlib.h"
#include "stdio.h"
#include "string.h"

#include "Table.h"
#include "Dictionary.h"
#include "Column.h"
#include "ColumnBase.h"

using namespace std;

using bigint = long long int;

template<class T>
bool contain(vector<T> &vec, T &item) {
	if (std::find(vec.begin(), vec.end(), item) != vec.end())
		return true;
	else
		return false;
}

vector<size_t> get_limit(vector<size_t>& input, size_t limit = 20) {
	vector<size_t> output;
	size_t size = input.size();
	limit = limit < size ? limit : size;
	for (size_t i = 0; i < limit; i++) {
		output.push_back(input.at(i));
	}
//	cout << "limit: " << output.size() << endl;
	input.clear();
	return output;
}

template<class T>
void print_result(vector<size_t>& input, Column<T>* col) {
	Dictionary<T>* dict = col->getDictionary();
	for (size_t i = 0; i < input.size(); i++) {
		dict->print(input.at(i));
//		cout << "packed value" << col->lookup_packed(i) << endl;
	}
}

// get memory status
int parseLine(char* line) {
	// This assumes that a digit will be found and the line ends in " Kb".
	int i = strlen(line);
	const char* p = line;
	while (*p < '0' || *p > '9')
		p++;
	line[i - 3] = '\0';
	i = atoi(p);
	return i;
}

int getMemory() { //Note: this value is in KB!
	FILE* file = fopen("/proc/self/status", "r");
	int result = -1;
	if (file == NULL) {
		perror("Can't read memory status");
	} else {
		char line[128];
		while (fgets(line, 128, file) != NULL) {
			if (strncmp(line, "VmRSS:", 6) == 0) {
				result = parseLine(line);
				break;
			}
		}
		fclose(file);
	}
	return result;
}

int main(void) {
	string prefix = "/home/alex/Documents/database/assignment2/raw";
	clock_t t1, t2;
	t1 = clock();
	int memory = getMemory();
	cout << "Memory status before: " << memory << "kb" << endl;
	string entity_path = prefix + "/sample-game.csv";
	string sensors_path = prefix + "/sensors.csv";
	string entities_path = prefix + "/entities.csv";

	// init column name
	vector<string> entity_name = { "eid", "type", "name" };
	vector<string> sensor_name = { "sid", "eid", "type" };
	vector<string> events_name = { "sid", "ts", "x", "y", "z", "v", "a", "vx",
			"vy", "vz", "ax", "ay", "az" };
	// init colummn type
	vector<ColumnBase::COLUMN_TYPE> events_type = { ColumnBase::uIntType,
			ColumnBase::llType, ColumnBase::intType, ColumnBase::intType,
			ColumnBase::intType, ColumnBase::uIntType, ColumnBase::uIntType,
			ColumnBase::intType, ColumnBase::intType, ColumnBase::intType,
			ColumnBase::intType, ColumnBase::intType, ColumnBase::intType };
	vector<ColumnBase::COLUMN_TYPE> sensor_type = { ColumnBase::uIntType,
			ColumnBase::uIntType, ColumnBase::uIntType };
	vector<ColumnBase::COLUMN_TYPE> entity_type = { ColumnBase::uIntType,
			ColumnBase::varcharType, ColumnBase::varcharType};
//	Table* events = new Table("events", &events_type, &events_name);
//	events->build_structure(entity_path);
	Table* entities = new Table("enities", &entity_type, &entity_name);
	entities->build_structure(entities_path);
//	Table* sensors = new Table("sensors", &sensor_type, &sensor_name);
//	sensors->build_structure(sensors_path);
	entities->print_table(20);

	t2 = clock();

	// create table

	return 0;

}
