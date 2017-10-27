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

#include "hsql/SQLParser.h"

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

// create a translation table
void create_translation(){

}
// combine result of a and b (row_id_a, row_id_b)

int main(void) {
	string prefix = "/home/alex/Documents/database/assignment2/raw";
	clock_t t1, t2;
	t1 = clock();
	int memory = getMemory();
	cout << "Memory status before: " << memory << "kb" << endl;

	const string query1 = "SELECT * from events JOIN sensors ON events.sid = sensors.sid";
	string query2 = "SELECT * from events JOIN sensors ON events.sid = sensors.sid WHERE sensors.type = 1 AND events.v > 5,000,000";
	string query3 = "SELECT * from events JOIN sensors ON events.sid = sensors.sid JOIN entities ON entities.eid = sensors.eid WHERE entities.name = “Ball 1” AND events.v > 5,000,000";

//	hsql::SQLParserResult result;
//	hsql::SQLParser::parse(query1, &result);

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


	Table* entities = new Table("enities", &entity_type, &entity_name);
	entities->build_structure(entities_path);
	Table* sensors = new Table("sensors", &sensor_type, &sensor_name);
	sensors->build_structure(sensors_path);
//	Table* events = new Table("events", &events_type, &events_name);
//	events->build_structure(entity_path);
//	sensors->print_table(20);
//	entities->print_table(20);
	cout << "Load done!";

	t2 = clock();

	// create translation
	unordered_map<size_t, size_t>* events_sensor;
	unordered_map<size_t, size_t>* final_result;

	// selection => actual value
	// select all mean get actual value

//	vector<unsigned int>* eid = events->select_all(0);
//	vector<unsigned int>* sid = sensors->select_all(0);
	vector<unsigned int>* eid;
	vector<unsigned int>* sid;
	// query 2

	// if search => need to look up
//	ColumnBase* col_b = events->getColumns()->at(5);
//	Column<unsigned int>* e_v = (Column<unsigned int>*) col_b;
	unsigned int input = 5000000U;
	vector<size_t> r_v;
//	e_v->getDictionary()->search(ColumnBase::gt, r_v, input);
	// sensor_type
	ColumnBase* col_b = sensors->getColumns()->at(2);
	Column<unsigned int>* s_type = (Column<unsigned int>*) col_b;
	vector<size_t> r_t;
	input = 1U;
	s_type->getDictionary()->search(ColumnBase::equal, r_t, input);

//	eid->clear();
//	sid->clear();

	sensors->lookup_id(r_t, 2, 0);

//	for(size_t i = 0; i < sid->size(); i++){
//		cout << sid->at(i);
//	}

	return 0;

}
