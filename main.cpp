#include <iostream>
#include <fstream>
#include <string>
#include <algorithm>
#include <ctime>

#include <boost/bimap.hpp>

#include "stdlib.h"
#include "stdio.h"
#include "string.h"

#include "Table.h"
#include "Dictionary.h"
#include "Column.h"
#include "ColumnBase.h"

using namespace std;

using bigint = long long int;

typedef boost::bimap<size_t, unsigned int> pos_id;
typedef boost::bimap<size_t, size_t> map_idx_idx;
typedef pos_id::value_type position;
typedef map_idx_idx::value_type idx2pos;

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
// join from eid_pos_value to sid_pos_value
pos_id create_tranlation_table(pos_id eid_pos_value, pos_id sid_pos_value){
	pos_id::right_const_iterator it;
	pos_id translation_table;
	for (pos_id::left_const_iterator pos_dict = eid_pos_value.left.begin(),
			iend = eid_pos_value.left.end(); pos_dict != iend; ++pos_dict) {
		it = sid_pos_value.right.find(pos_dict->second);
		if (it != sid_pos_value.right.end()) {
			// find actual value in b
			translation_table.insert(position(pos_dict->first, it->first));
		}
	}
	return translation_table;
}

void print_translation_table(pos_id translation_table){
	// print translation table
	cout << "| A | B |" << endl;
	for (pos_id::left_const_iterator pos_dict = translation_table.left.begin(),
			iend = translation_table.left.end(); pos_dict != iend; ++pos_dict) {
		cout << pos_dict->first << "|" << pos_dict->second << "|" << endl;
	}
}

void print_query_result(map<size_t, size_t> e_row_dict, pos_id translation_table, int limit){
	pos_id::left_const_iterator itb;
	size_t i = 0;
	cout << "size of table result:  << " << e_row_dict.size() << endl;
	map<size_t, size_t>::iterator ita;
	for (ita = e_row_dict.begin(); ita != e_row_dict.end(); ita++) {
		itb = translation_table.left.find(ita->second);
		if (itb != translation_table.left.end() && i < limit) {
			// find row id in b
			cout << "|" << i << "|" << ita->first << "|" << endl;
			i++;
		}
	}
}

void print_query_result_mul(map<size_t, size_t> e_row_dict, pos_id translation_table1, pos_id translation_table2, int limit){
	pos_id::left_const_iterator itb;
	size_t i = 0;
	cout << "size of table result:  << " << e_row_dict.size() << endl;
	map<size_t, size_t>::iterator ita;
	for (ita = e_row_dict.begin(); ita != e_row_dict.end(); ita++) {
		itb = translation_table1.left.find(ita->second);
		if (itb != translation_table1.left.end() && i < limit) {
			itb = translation_table2.left.find(itb->second);
			if(itb != translation_table2.left.end() && i < limit){
				// find row id in b
				cout << "|" << i << "|" << ita->first << "|" << endl;
				i++;
			}
		}
	}
}


int main(void) {
	int limit = 20;
	string prefix = "/home/alex/Documents/database/assignment2/raw";
	clock_t t1, t2;
	t1 = clock();
	int memory = getMemory();
	cout << "Memory status before: " << memory << "kb" << endl;

	const string query1 =
			"SELECT * from events JOIN sensors ON events.sid = sensors.sid";
	string query2 =
			"SELECT * from events JOIN sensors ON events.sid = sensors.sid WHERE sensors.type = 1 AND events.v > 5,000,000";
	string query3 =
			"SELECT * from events JOIN sensors ON events.sid = sensors.sid JOIN entities ON entities.eid = sensors.eid WHERE entities.name = “Ball 1” AND events.v > 5,000,000";

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
			ColumnBase::varcharType, ColumnBase::varcharType };

	Table* entities = new Table("enities", &entity_type, &entity_name);
	entities->build_structure(entities_path);
	Table* sensors = new Table("sensors", &sensor_type, &sensor_name);
	sensors->build_structure(sensors_path);
	Table* events = new Table("events", &events_type, &events_name);
	events->build_structure(entity_path);
	cout << "Load done!" << endl;
	t2 = clock();

	// query 1
//	map<size_t, size_t> e_row_dict1;
//	vector<size_t>* e_row_id1 = NULL;
//	pos_id e1_pos_value = events->select_all(0, e_row_dict1, e_row_id1);
//
//	map<size_t, size_t> s_row_dict1;
//	vector<size_t>* s_row_id1 = NULL;
//	pos_id s1_pos_value = sensors->select_all(0, s_row_dict1, s_row_id1);
//	pos_id translation_table1 = create_tranlation_table(e1_pos_value, s1_pos_value);
//	print_translation_table(translation_table1);
//	print_query_result(e_row_dict1, translation_table1, limit);
	// query 2

	// if search => need to look up
//	int e_sel = 5;
//	ColumnBase* col_b = events->getColumns()->at(e_sel);
//	Column<unsigned int>* e_v = (Column<unsigned int>*) col_b;
//	unsigned int input = 5000000U;
//	vector<size_t> r_v;
//	e_v->getDictionary()->search(ColumnBase::gt, r_v, input);

//	map<size_t, size_t> e_row_dict;
//	vector<size_t>* e_row_id = NULL;
//	pos_id eid_pos_value = events->lookup_id(r_v, e_sel, 0, e_row_dict, e_row_id);

//	// sensor_type
//	int s_sel = 2;
//	col_b = sensors->getColumns()->at(2);
//	Column<unsigned int>* s_type = (Column<unsigned int>*) col_b;
//	vector<size_t> r_t;
//	input = 2U;
//	s_type->getDictionary()->search(ColumnBase::equal, r_t, input);
//
//	t2 = clock();
//	// map from row id to dict position on sid
//	map<size_t, size_t> s_row_dict;
//	vector<size_t>* s_row_id = NULL;
//	pos_id sid_pos_value = sensors->lookup_id(r_t, s_sel, 0, s_row_dict, s_row_id);
//
//	//iterate a look up b
//	//create translation table
//	pos_id translation_table = create_tranlation_table(eid_pos_value, sid_pos_value);
//	print_translation_table(translation_table);
//	// final result of query
//	// loop a rowid dict pos then find in translation table dictionary pos of b
//	print_query_result(e_row_dict, translation_table, limit);

	// query 3
	// selection in events
	int e_sel = 5;
	ColumnBase* col_b = events->getColumns()->at(e_sel);
	Column<unsigned int>* e_v = (Column<unsigned int>*) col_b;
	unsigned int input = 5000000U;
	vector<size_t> r_v;
	e_v->getDictionary()->search(ColumnBase::gt, r_v, input);

	map<size_t, size_t> e_row_dict;
	vector<size_t>* e_row_id = NULL;
	pos_id eid_pos_value = events->lookup_id(r_v, e_sel, 0, e_row_dict, e_row_id);

	// select all sensors
	map<size_t, size_t> s_row_dict3;
	vector<size_t>* s_row_id3 = NULL;
	pos_id s3_pos_value = sensors->select_all(0, s_row_dict3, s_row_id3);
	// translation for envents to sensors
	pos_id translation_table_evs = create_tranlation_table(eid_pos_value, s3_pos_value);
	print_translation_table(translation_table_evs);

	// join to entities
	map<size_t, size_t> set_row_dict;
	vector<size_t>* set_row_id = NULL;
	pos_id etid_pos_value = sensors->select_all(1, set_row_dict, set_row_id);
	// select entities
	e_sel = 2;
	col_b = entities->getColumns()->at(e_sel);
	Column<string>* et_name = (Column<string>*) col_b;
	string name = "Ball 1";
	vector<size_t> r_et;
	et_name->getDictionary()->search(ColumnBase::equal, r_et, name);
	map<size_t, size_t> et_row_dict3;
	vector<size_t>* et_row_id3 = NULL;
	pos_id et_pos_value = entities->lookup_id(r_et, e_sel, 0, et_row_dict3, et_row_id3);
	pos_id translation_table_set = create_tranlation_table(etid_pos_value, et_pos_value);
	print_translation_table(translation_table_set);
	print_query_result_mul(e_row_dict, translation_table_evs, translation_table_set, limit);

	return 0;
}
