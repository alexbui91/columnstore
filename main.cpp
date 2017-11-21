#include <iostream>
#include <fstream>
#include <string>
#include <thread>
#include <algorithm>
#include <ctime>

#include <boost/bimap.hpp>
#include <boost/algorithm/string.hpp>

#include "sql/SQLParser.h"
#include "sql/util/sqlhelper.h"

#include "string.h"

#include "Table.h"
#include "Dictionary.h"
#include "Column.h"
#include "ColumnBase.h"
#include "Expr.h"
#include "server.h"

using namespace std;

using bigint = long long int;

typedef boost::bimap<size_t, unsigned int> pos_id;
typedef boost::bimap<size_t, size_t> map_idx_idx;
typedef pos_id::value_type position;
typedef map_idx_idx::value_type idx2pos;

const string prefix = "/home/alex/Documents/database/assignment2/raw";
//	string prefix = "/Users/alex/Documents/workspacecplus/columnstore/data";

const int garbage_interval = 10;
const bool debug = false;
const bool res_full = true;

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
	string col_name = "";
	for(size_t c = 0; c < table->getColumns()->size(); c++){
		col_name = table->getColumns()->at(c)->getName();
		table->pad_string(col_name, 20);
		tmp.append(col_name);
	}
	cout << "||Sq|" << tmp << endl;
	col_name = "";
	for(size_t j = 0; j < tmp.size(); j++){
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
			if(i < (size_t) limit){
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

// deactivate transaction
void collect_garbage(size_t& tx, Table *table, vector<bool>* q_resultRid, vector<Expr*>* updates){
//	int fl = 0;
//	int total_effect = 0;
	int flag = false;
	size_t sz = q_resultRid->size();
	ColumnBase* colBase;
	for(size_t i = 0; i < sz; i++){
		flag = q_resultRid->at(i);
		if(flag){
			for(Expr* e : *updates){
				colBase = table->getColumnByName(e->getField());
				switch(colBase->getType()){
				case ColumnBase::uIntType: {
					Column<unsigned int>* t = (Column<unsigned int>*) colBase;
//					fl = t->collect_garbage(i);
					t->deactivate_transaction(tx, i);
					break;
				}
				case ColumnBase::intType: {
					Column<int>* t = (Column<int>*) colBase;
//					fl = t->collect_garbage(i);
					t->deactivate_transaction(tx, i);
					break;
				}
				case ColumnBase::llType: {
					Column<bigint>* t = (Column<bigint>*) colBase;
//					fl = t->collect_garbage(i);
					t->deactivate_transaction(tx, i);
					break;
				}
				default:
					Column<string>* t = (Column<string>*) colBase;
//					fl = t->collect_garbage(i);
					t->deactivate_transaction(tx, i);
					break;
				}
			}
//			total_effect += fl;
		}
	}
//	if(total_effect)
//		cout << total_effect << " versions have been collected" << endl;
}
string projection(Table* table, size_t& tx){
	size_t limit_count = 0;
	size_t limit = 20;
	string tmp = "";
	string col_name = "";
	for(size_t i = 0; i < table->getLength(); i++){
		col_name.append("||");
		if(limit_count >= 9){
			col_name.append(to_string(limit_count + 1));
		}else{
			col_name.append(to_string(limit_count + 1).append(" "));
		}
		col_name += table->get_data_by_row(i, tx) + "|\n";
		limit_count++;
		if(limit_count >= limit)
			break;
	}
	tmp += col_name;
	return tmp;
}
string projection(Table* table, size_t& tx, vector<bool>* q_resultRid){
	bool flag = false;
	size_t limit_count = 0;
	size_t limit = 20;
	string tmp = "";
	string col_name = "";
	for(size_t i = 0; i < q_resultRid->size(); i++){
		flag = q_resultRid->at(i);
		if(flag){
			col_name.append("||");
			if(limit_count >= 9){
				col_name.append(to_string(limit_count + 1));
			}else{
				col_name.append(to_string(limit_count + 1).append(" "));
			}
			col_name += table->get_data_by_row(i, tx) + "|\n";
			limit_count++;
			if(limit_count >= limit)
				break;
		}
	}
	tmp += col_name;
	return tmp;
}

vector<bool>* execute_select(size_t& tx, Table* table, vector<Expr*>* list_expr, bool res=true){
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
			t->selection(searchValue, q_where_op, q_resultRid, tx, initQueryResult);
		}else if(colBase->getType() == ColumnBase::COLUMN_TYPE::uIntType){
			Column<unsigned int>* t = (Column<unsigned int>*) colBase;
			unsigned int searchValue = 0U;
			try {
				searchValue = stoi(q_where_value);
			} catch (exception& e) {
				cerr << "Exception: " << e.what() << endl;
			}
			t->selection(searchValue, q_where_op, q_resultRid, tx, initQueryResult);
		}else if(colBase->getType() == ColumnBase::COLUMN_TYPE::llType){
			Column<bigint>* t = (Column<bigint>*) colBase;
			bigint searchValue = 0ll;
			try {
				searchValue = stoll(q_where_value);
			} catch (exception& e) {
				cerr << "Exception: " << e.what() << endl;
			}
			t->selection(searchValue, q_where_op, q_resultRid, tx, initQueryResult);
		}else{
			Column<string>* t = (Column<string>*) colBase;
			string searchValue = q_where_value;
			t->selection(searchValue, q_where_op, q_resultRid, tx, initQueryResult);
		}
	}
	if(res){
		collect_garbage(tx, table, q_resultRid, list_expr);
	}
	return q_resultRid;
}
// execute select query after parse
vector<bool>* execute_select(Table* table, vector<Expr*>* list_expr, bool res=true){
	size_t tx = utils::get_timestamp();
	return execute_select(tx, table, list_expr, res);
}
// execute update query
long execute_update(Table* table, vector<Expr*>* list_expr, vector<Expr*>* updates){
//	ColumnBase* colBase = table->getColumnByName("sid");
//	Column<unsigned int>* t = (Column<unsigned int>*) colBase;
//	cout << t->getDictionary()->getItems()->size() << endl;
	size_t tx = utils::get_timestamp();
	vector<bool>* q_resultRid = execute_select(tx, table, list_expr, false);
	size_t sz = q_resultRid->size();
//	cout << "Finish scan: " << sz << " records" << endl;
	size_t total_effect = 0;
	if(sz){
		bool flag = false;
		ColumnBase* colBase;
		for(size_t i = 0; i < sz; i++){
			flag = q_resultRid->at(i);
			if(flag){
				total_effect++;
				for(Expr* e : *updates){
					colBase = table->getColumnByName(e->getField());
					switch(colBase->getType()){
					case ColumnBase::uIntType: {
						Column<unsigned int>* t = (Column<unsigned int>*) colBase;
						unsigned int value = stoi(e->getVal());
						t->create_version(tx, i, value);
						break;
					}
					case ColumnBase::intType: {
						Column<int>* t = (Column<int>*) colBase;
						int value = stoi(e->getVal());
						t->create_version(tx, i, value);
						break;
					}
					case ColumnBase::llType: {
						Column<bigint>* t = (Column<bigint>*) colBase;
						bigint value = stoll(e->getVal());
						t->create_version(tx, i, value);
						break;
					}
					default:
						Column<string>* t = (Column<string>*) colBase;
						string value = e->getVal();
						t->create_version(tx, i, value);
						break;
					}
				}
			}
		}
		if(total_effect)
			utils::print(to_string(total_effect) + " versions are created");
		// commit and garbage collection
		collect_garbage(tx, table, q_resultRid, updates);
	}
	return total_effect;
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
	else if (literalType == hsql::ExprType::kExprLiteralFloat)
		val = to_string(expr->expr2->fval);
	else if (literalType == hsql::ExprType::kExprColumnRef)
		val = expr->expr2->name;
	Expr* aexpr = new Expr(val, ename, op);
	return aexpr;
}
// add expression for set clause in update
Expr* add_reg_ops(hsql::UpdateClause* s){
	string col = s->column;
	string val;
	hsql::ExprType literalType = s->value->type;
	if (literalType == hsql::ExprType::kExprLiteralInt)
		val = to_string(s->value->ival);
	else if (literalType == hsql::ExprType::kExprLiteralFloat)
		val = to_string(s->value->fval);
	else if (literalType == hsql::ExprType::kExprColumnRef)
		val = s->value->name;
	ColumnBase::OP_TYPE op = ColumnBase::none;
	Expr* aexpr = new Expr(val, col, op);
	return aexpr;
}
// get expression in where clause
void get_where_expr(hsql::Expr* expr, vector<Expr*>* list_expr){
	Expr* n_e;
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
// parse sql statement
string parse_sql(const string &query, map<string, Table*>* list_tables, vector<string> &q_select_fields, vector<Expr*>* list_expr, vector<Expr*>* updates){
	string tname = "";
	hsql::SQLParserResult result;
	hsql::SQLParser::parse(query, &result);
	if (result.isValid() && result.size() > 0) {
		const hsql::SQLStatement* statement = result.getStatement(0);
		if (statement->isType(hsql::kStmtSelect)) {
			const hsql::SelectStatement* select = (const hsql::SelectStatement*) statement;
			tname = select->fromTable->getName();
			map<string, Table*>::iterator it;
			it = list_tables->find(tname);
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
					get_where_expr(expr, list_expr);
				}
			}
		}else if(statement->isType(hsql::kStmtUpdate)){
			const hsql::UpdateStatement* select = (const hsql::UpdateStatement*) statement;
			tname = select->table->name;
			map<string, Table*>::iterator it;
			it = list_tables->find(tname);
			if (it != list_tables->end()){
				// process where clause
				if (select->where != NULL) {
					hsql::Expr* expr = select->where;
					get_where_expr(expr, list_expr);
				}
				// process set clause
				Expr* n_e;
				for(hsql::UpdateClause* s : *select->updates){
					// in case update has expression, now not processed
//					cout << s->value->opType;
					// process simple ones
					n_e = add_reg_ops(s);
					updates->push_back(n_e);
				}
			}
		}
	}
	return tname;
}
// parse for select
string parse_sql(const string &query, map<string, Table*>* list_tables,vector<Expr*>* list_expr, vector<Expr*>* updates){
	vector<string> q_select_fields;
	return parse_sql(query, list_tables, q_select_fields, list_expr, updates);
}
// parse for update
string parse_sql(const string &query, map<string, Table*>* list_tables, vector<string> &q_select_fields, vector<Expr*>* list_expr){
	vector<Expr*>* updates = new vector<Expr*>();
	string table = parse_sql(query, list_tables, q_select_fields, list_expr, updates);
	delete updates;
	return table;
}
/*
 * get total records of query select
 * */
long get_total_result(vector<bool>* results){
	size_t total = 0;
	for(size_t i = 0; i < results->size(); i++){
		if(results->at(i)){
			total++;
		}
	}
	return total;
}
void process_incomming_client(Server* server, int &inc, string &mes, Table *table, map<string, Table*>* list_tables){
	string res = "";
	vector<string> q_select_fields;
	vector<Expr*>* list_expr = new vector<Expr*>();
	boost::algorithm::to_lower(mes);
	string table_name;
	long total = 0;
	if(utils::start_with(mes, "update")){
		vector<Expr*>* updates = new vector<Expr*>();
		table_name = parse_sql(mes, list_tables, list_expr, updates);
		if(!table_name.empty()){
			utils::print("Execute query: " + mes);
			total = execute_update(table, list_expr, updates);
			res = to_string(total) + " records are updated";
		}else{
			res = "There are errors syntax in your query";
		}
	}else if(utils::start_with(mes, "select")){
		table_name = parse_sql(mes, list_tables, q_select_fields, list_expr);
		if(!table_name.empty()){
			utils::print("Execute query: " + mes);
			size_t tx = utils::get_timestamp();
			if(list_expr->empty()){
				res = projection(table, tx);
//				cout << res << endl;
				res += to_string(table->getLength()) + " records are found";
			}else{
				vector<bool>* q_result = execute_select(tx, table, list_expr);
				total = get_total_result(q_result);
				res = projection(table, tx, q_result);
				res += to_string(total) + " records are found";
			}
		}else{
			res = "There are errors syntax in your query";
		}
	}
	delete list_expr;
	if(!debug){
		server->sendMessage(inc, res.c_str());
		server->closeConnection(inc);
	}
}
void update_table_space(Table* table){
	try{
		clock_t t1 = clock();
		int t = 0;
		string mes;
		long total = 0;
		while(true){
			t = (static_cast<int>(((float) clock() - (float) t1) / CLOCKS_PER_SEC)) / garbage_interval;
			if(t == 1){
				total = 0;
				t1 = clock();
				for(ColumnBase* colBase : *table->getColumns()){
					switch(colBase->getType()){
					case ColumnBase::uIntType: {
						Column<unsigned int>* t = (Column<unsigned int>*) colBase;
						total = t->update_latest_version();
						break;
					}
					case ColumnBase::intType: {
						Column<int>* t = (Column<int>*) colBase;
						total = t->update_latest_version();
						break;
					}
					case ColumnBase::llType: {
						Column<bigint>* t = (Column<bigint>*) colBase;
						total = t->update_latest_version();
						break;
					}
					default:
						Column<string>* t = (Column<string>*) colBase;
						total = t->update_latest_version();
						break;
					}
					if(total)
						utils::print(to_string(total) + " versions have been collected");
				}
			}
		}
	}catch(exception& e){
		cerr << "Exception in garbage collection: " << e.what() << endl;
	}

}
void start_server(int port, Table* table, map<string, Table*>* list_tables){
	utils::print("Start server");
	Server* server = new Server(port);
	if(!debug)
		server->initServer();
	int inc = 0;
	try{
		if(debug){
			string mes = "UPDATE events SET sid = 62 WHERE sid = 40";
			string mes2 = "select * from events";
			process_incomming_client(server, inc, mes, table, list_tables);
			process_incomming_client(server, inc, mes2, table, list_tables);
		}
		else{
			string mes = "";
			char buffer[256];
			int activity = 0;
			while (true) {
				//clear the socket set
				server->clear_set();
				server->add_connection(server->get_master_socket());
				server->check_valid_connection();
				activity = server->wait_connection();
				if ((activity < 0) && (errno!=EINTR)){
					printf("select error");
				}
				if(server->is_valid_master()){
					inc = server->openConnection(buffer);
					string bu(buffer);
					mes = bu;
					process_incomming_client(server, inc, mes, table, list_tables);
				}
			}
		}
	}catch(exception& e){
		server->closeServer();
		cerr << "Exception in server runtime: " << e.what() << endl;
	}
}
int main(int argc, char *argv[]) {
	int port = 8888;
	if(argc > 1){
		port = atoi(argv[1]);
	}
	float memory = getMemory();
	utils::print("Memory status at the starting point: " + to_string(memory) + "Mb");
	string entity_path;
	if(!debug)
		entity_path = prefix + "/sample-game.csv";
	else
		entity_path = prefix + "/test.csv";
	string sensors_path = prefix + "/sensors.csv";
	string entities_path = prefix + "/entities.csv";
	vector<ColumnBase::COLUMN_TYPE> events_type = { ColumnBase::uIntType,
			ColumnBase::llType, ColumnBase::intType, ColumnBase::intType,
			ColumnBase::intType, ColumnBase::uIntType, ColumnBase::uIntType,
			ColumnBase::intType, ColumnBase::intType, ColumnBase::intType,
			ColumnBase::intType, ColumnBase::intType, ColumnBase::intType };
	vector<string> events_name = { "sid", "ts", "x", "y", "z", "v", "a", "vx",
			"vy", "vz", "ax", "ay", "az" };
	map<string, Table*>* list_tables = new map<string, Table*>();
	Table* events = new Table("events", &events_type, &events_name);
	events->build_structure(entity_path);
	utils::print("Load done");
	list_tables->insert(pair<string, Table*>("events", events));
	thread server = thread(start_server, ref(port), ref(events), ref(list_tables));
	thread garbage = thread(update_table_space, ref(events));
	server.join();
	garbage.join();

	return 0;
}
