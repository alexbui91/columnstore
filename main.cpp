#include <iostream>
#include <fstream>
#include <string>
#include <algorithm>
#include <ctime>

#include <boost/bimap.hpp>

#include "sql/SQLParser.h"
#include "sql/util/sqlhelper.h"

#include "string.h"

#include "Table.h"
#include "Dictionary.h"
#include "Column.h"
#include "ColumnBase.h"
#include "Expr.h"

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

float getMemory() { //Note: this value is in KB!
	FILE* file = fopen("/proc/self/status", "r");
	float result = 0.0f;
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
	if(result){
		result = result / 1024;
	}
	return result;
}

float get_time(clock_t &t){
	return ((float) clock() - (float) t) / CLOCKS_PER_SEC;
}

// create a translation table
// join from eid_pos_value to sid_pos_value
pos_id create_tranlation_table(pos_id eid_pos_value, pos_id sid_pos_value){
	pos_id::right_const_iterator it;
	pos_id translation_table;
	for (pos_id::left_const_iterator pos_dict = eid_pos_value.left.begin(),
			iend = eid_pos_value.left.end(); pos_dict != iend; ++pos_dict) {
		it = sid_pos_value.right.find(pos_dict->second);
//		cout << pos_dict->first << " | " << pos_dict->second;
		if (it != sid_pos_value.right.end()) {
			// find actual value in b
			translation_table.insert(position(pos_dict->first, it->second));
		}
	}
	return translation_table;
}

// using master slave in translation step
void create_translation_table_slave(){

}

void print_translation_table(pos_id translation_table, string mes=""){
	// print translation table
	cout << mes << endl;
	cout << "| A | B |" << endl;
	for (pos_id::left_const_iterator pos_dict = translation_table.left.begin(),
			iend = translation_table.left.end(); pos_dict != iend; ++pos_dict) {
		cout << pos_dict->first << "|" << pos_dict->second << "|" << endl;
	}
}

void print_query_result(Table* table, map<size_t, size_t> e_row_dict, pos_id translation_table, int limit){
	cout << "Query results: " << endl;
	pos_id::left_const_iterator itb;
	size_t i = 0;
	map<size_t, size_t>::iterator ita;
	string tmp = "";
	int size = 0;
	string col_name = "";
	int j = 0;
	for(int c = 0; c < table->getColumns()->size(); c++){
		col_name = table->getColumns()->at(c)->getName();
		table->pad_string(col_name, 20);
		tmp.append(col_name);
	}
	cout << "||Sq|" << tmp << endl;
	col_name = "";
	for(j = 0; j < tmp.size(); j++){
		col_name.append("-");
	}
	cout << "-----" << col_name << endl;
	tmp.clear();
	col_name.clear();
	for (ita = e_row_dict.begin(); ita != e_row_dict.end(); ita++) {
		itb = translation_table.left.find(ita->second);
//		cout << ita->second << "||" << itb->first << "||" << itb ->second << endl;
		if (itb != translation_table.left.end()) {
			col_name.clear();
			// find row id in b
			i++;
			if(i < limit){
//				cout << "|" << i << "|" << (ita->first + 1) << "|" << endl;
				col_name.append("||");
				if(i >= 10){
					col_name.append(to_string(i));
				}else{
					col_name.append(to_string(i).append(" "));
				}
				col_name.append(table->get_data_by_row(ita->first)) ;
				col_name.append("|\n");
				tmp.append(col_name);
			}
		}
	}
	cout << tmp << endl;
	cout << "Total records avalable: " << i << endl;
}

// execute select query after parse

vector<bool>* execute_select(Table* table, vector<Expr*>* list_expr){
	vector<bool>* q_resultRid = new vector<bool>();
	string q_where_value;
	Expr* e;
	bool initQueryResult = false;
	for (size_t i = 0; i < list_expr->size(); i++) {
		e = list_expr->at(i);
		ColumnBase::OP_TYPE q_where_op = e->getOp();
		q_where_value = e->getVal();
		// get column by name then cast to appropriate column based on column type
		ColumnBase* colBase = table->getColumnByName(e->getField());
		initQueryResult = (i == 0);
		if (colBase == NULL) continue;
		if(colBase->getType() == ColumnBase::COLUMN_TYPE::intType){
			Column<int>* t = (Column<int>*) colBase;
			int searchValue = 0;
			try {
				searchValue = stoi(q_where_value);
			} catch (exception& e) {
				cerr << "Exception: " << e.what() << endl;
			}
			t->selection(searchValue, q_where_op, q_resultRid, initQueryResult);
		}else if(colBase->getType() == ColumnBase::COLUMN_TYPE::uIntType){
			Column<unsigned int>* t = (Column<unsigned int>*) colBase;
			unsigned int searchValue = 0U;
			try {
				searchValue = stoi(q_where_value);
			} catch (exception& e) {
				cerr << "Exception: " << e.what() << endl;
			}
			t->selection(searchValue, q_where_op, q_resultRid, initQueryResult);
		}else if(colBase->getType() == ColumnBase::COLUMN_TYPE::llType){
			Column<bigint>* t = (Column<bigint>*) colBase;
			bigint searchValue = 0ll;
			try {
				searchValue = stoll(q_where_value);
			} catch (exception& e) {
				cerr << "Exception: " << e.what() << endl;
			}
		}else{
			Column<string>* t = (Column<string>*) colBase;
			string searchValue = q_where_value;
			t->selection(searchValue, q_where_op, q_resultRid, initQueryResult);
		}
	}

	return q_resultRid;
}
/* get total row of query result */
size_t get_total_count(vector<bool>* q_resultRid){
	size_t totalResult = 0;
	for (size_t rid = 0; rid < q_resultRid->size(); rid++) {
		if (q_resultRid->at(rid))
			++totalResult;
	}
	return totalResult;
}

Expr* add_reg_ops(hsql::Expr* expr){
	string ename = expr->expr->getName();
	ColumnBase::OP_TYPE op;
	if (expr->opType == hsql::OperatorType::kOpGreater) {
		op = ColumnBase::OP_TYPE::gt;
	}else if(expr->opType == hsql::OperatorType::kOpGreaterEq){
		op = ColumnBase::OP_TYPE::ge;
	}else if(expr->opType == hsql::OperatorType::kOpLess){
		op = ColumnBase::OP_TYPE::lt;
	}else if(expr->opType == hsql::OperatorType::kOpLessEq){
		op = ColumnBase::OP_TYPE::le;
	}else if(expr->opType == hsql::OperatorType::kOpEquals){
		op = ColumnBase::OP_TYPE::equal;
	}
	string val;
	hsql::ExprType literalType = expr->expr2->type;
	if (literalType == hsql::ExprType::kExprLiteralInt)
		val = to_string(expr->expr2->ival);
	else if (literalType == hsql::ExprType::kExprColumnRef)
		val = expr->expr2->name;
	Expr* aexpr = new Expr(val, ename, op);
	return aexpr;
}

// parse sql statement
string parse_sql(const string &query, map<string, Table*>* list_tables, vector<string> &q_select_fields, vector<Expr*>* list_expr){
	string tname = "";
	hsql::SQLParserResult result;
	hsql::SQLParser::parse(query, &result);
	Expr* n_e;
	if (result.isValid() && result.size() > 0) {
		const hsql::SQLStatement* statement = result.getStatement(0);
		if (statement->isType(hsql::kStmtSelect)) {
			const hsql::SelectStatement* select = (const hsql::SelectStatement*) statement;
			tname = select->fromTable->getName();
			map<string, Table*>::iterator it;
			it = list_tables->find(tname);
			vector<bool>* q_resultRid = new vector<bool>();
			if (it != list_tables->end()){
				Table* table = it->second;
				for (hsql::Expr* expr : *select->selectList) {
					if (expr->type == hsql::ExprType::kExprStar) {
						for (ColumnBase* colBase : *table->getColumns()) {
							q_select_fields.push_back(colBase->getName());
						}
					}
				}
				if (select->whereClause != NULL) {
					hsql::Expr* expr = select->whereClause;
					if (expr->type == hsql::ExprType::kExprOperator) {
						if (expr->opType == hsql::OperatorType::kOpAnd){
							n_e = add_reg_ops(expr->expr);
							list_expr->push_back(n_e);
							if (expr->expr2 != nullptr) {
								n_e = add_reg_ops(expr->expr2);
								list_expr->push_back(n_e);
							} else if (expr->exprList != nullptr) {
								for (hsql::Expr* e : *expr->exprList) {
									n_e = add_reg_ops(e);
									list_expr->push_back(n_e);
								}
							}
						} else{
							n_e = add_reg_ops(expr);
							list_expr->push_back(n_e);
						}
					}
				}
			}

		}
	}else {
		fprintf(stderr, "%s (L%d:%d)\n", result.errorMsg(),
				result.errorLine(), result.errorColumn());
	}
//	delete n_e;
	return tname;
}
int main(void) {
	const string prefix = "/home/alex/Documents/database/assignment2/raw";
//	string prefix = "/Users/alex/Documents/workspacecplus/columnstore/data";
	clock_t t2;
	t2 = clock();
	float memory = getMemory();
	cout << "Memory status at the starting point: " << memory << "Mb" << endl;

	const string query1 = "SELECT * from events where events.sid = 40";
	const string query2 = "SELECT * from events where events.v > 5000000";
	const string query3 = "SELECT * from events where events.ts > 1000000000000000 and events.ts = 12100000000000000";

	string entity_path = prefix + "/sample-game.csv";
	string sensors_path = prefix + "/sensors.csv";
	string entities_path = prefix + "/entities.csv";

	// init column name
	vector<string> events_name = { "sid", "ts", "x", "y", "z", "v", "a", "vx",
			"vy", "vz", "ax", "ay", "az" };

	// init colummn type
	vector<ColumnBase::COLUMN_TYPE> events_type = { ColumnBase::uIntType,
			ColumnBase::llType, ColumnBase::intType, ColumnBase::intType,
			ColumnBase::intType, ColumnBase::uIntType, ColumnBase::uIntType,
			ColumnBase::intType, ColumnBase::intType, ColumnBase::intType,
			ColumnBase::intType, ColumnBase::intType, ColumnBase::intType };
	map<string, Table*>* list_tables = new map<string, Table*>();
	Table* events = new Table("events", &events_type, &events_name);
	events->build_structure(entity_path);
	list_tables->insert(pair<string, Table*>("events", events));
	vector<Expr*>* list_expr = new vector<Expr*>();
	vector<string> q_select_fields;
	string table_name = parse_sql(query1, list_tables, q_select_fields, list_expr);
	vector<bool>* q_result;
	if(!table_name.empty()){
		q_result = execute_select(events, list_expr);
		cout << q_result->size() << endl;
	}
	list_expr->clear();
	q_select_fields.clear();
	table_name = parse_sql(query2, list_tables, q_select_fields, list_expr);
	// query 3
	list_expr->clear();
	q_select_fields.clear();
	table_name = parse_sql(query3, list_tables, q_select_fields, list_expr);
	// parse a given query

	return 0;
}
