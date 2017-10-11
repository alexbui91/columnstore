#include <iostream>
#include <fstream>
#include <string>
#include <algorithm>
#include <ctime>

#include "stdlib.h"
#include "stdio.h"
#include "string.h"

#include "Dictionary.h"
#include "Column.h"
#include "ColumnBase.h"

using namespace std;

template <class T>
bool contain(vector<T> &vec, T &item){
	if(std::find(vec.begin(), vec.end(), item) != vec.end())
		return true;
	else
		return false;
}

vector<size_t> get_limit(vector<size_t>& input, size_t limit = 20){
	vector<size_t> output;
	size_t size = input.size();
	limit = limit < size ? limit : size;
	for(size_t i = 0; i < limit; i++){
		output.push_back(input.at(i));
	}
//	cout << "limit: " << output.size() << endl;
	input.clear();
	return output;
}

template <class T>
void print_result(vector<size_t>& input, Column<T>* col){
	Dictionary<T>* dict = col->getDictionary();
	for(size_t i = 0; i < input.size(); i++){
		dict->print(input.at(i));
//		cout << "packed value" << col->lookup_packed(i) << endl;
	}
}

using bigint = long long int;
vector<size_t> col_p = {0, 1, 5};
vector<ColumnBase::COLUMN_TYPE> col_type = {ColumnBase::uIntType, ColumnBase::llType, ColumnBase::uIntType};

// get memory status
int parseLine(char* line){
    // This assumes that a digit will be found and the line ends in " Kb".
    int i = strlen(line);
    const char* p = line;
    while (*p <'0' || *p > '9') p++;
    line[i-3] = '\0';
    i = atoi(p);
    return i;
}

int getMemory(){ //Note: this value is in KB!
	FILE* file = fopen("/proc/self/status", "r");
	int result = -1;
	char line[128];

	while (fgets(line, 128, file) != NULL){
		if (strncmp(line, "VmRSS:", 6) == 0){
			result = parseLine(line);
			break;
		}
	}
	fclose(file);
	return result;
}

long to_lossy(string temp_col){
	return stol(temp_col.substr(0, temp_col.length() / 2));
}

int main(void){
	clock_t t1,t2;
	t1=clock();
	int memory = getMemory();
	cout << "Memory status before: " << memory << "kb" << endl;
	string path = "/home/alex/Documents/database/assignment2/raw/sample-game.csv";
	ifstream infile(path);
	if (!infile) {
		cout << "Cannot open file: " << path << endl;
		return -1;
	}
	string line;
	string delim = ",";
//	vector<size_t> col_p = {0, 1, 5};
	vector<string> col_name = {"sid", "ts", "v"};
//	vector<ColumnBase::COLUMN_TYPE> col_type = {ColumnBase::uIntType, ColumnBase::llType, ColumnBase::uIntType};
	vector<ColumnBase*> columns;
	string name;
	size_t t_index = 0;
	for (auto i = col_p.begin(); i != col_p.end(); i++){
		ColumnBase* col_b = new ColumnBase();
		name = col_name.at(t_index);
		switch(col_type.at(t_index)){
			case ColumnBase::uIntType:{
				Column<unsigned int>* col = new Column<unsigned int>();
				col_b = col;
				break;
			}
			case ColumnBase::intType:{
				Column<int>* col2 = new Column<int>();
				col_b = col2;
				break;
			}
			case ColumnBase::llType:{
				Column<bigint>* col3 = new Column<bigint>();
				col_b = col3;
				break;
			}
			default:
				Column<string>* col4 = new Column<string>();
				col_b = col4;
				break;
		}
		col_b->setName(name);
		col_b->setType(col_type.at(t_index));
		columns.push_back(col_b);
		t_index++;
	}
	// init ts lossy col
	Column<long>* ts_lossy = new Column<long>();

	// load data into corresponding column
	while(getline(infile, line)) {
		size_t last = 0; size_t next = 0;
		// count total column
		size_t c_index = 0;
		// count selected column
		t_index = 0;
		while((next = line.find(delim, last)) != string::npos){
			if(contain(col_p, c_index)){
				string temp_col = line.substr(last, next - last);
				ColumnBase* col_b = columns.at(t_index);
				switch(col_b->getType()){
					case ColumnBase::uIntType:{
						Column<unsigned int>* col = (Column<unsigned int>*) col_b;
						unsigned int cell = (unsigned int) stoi(temp_col);
						col->updateDictionary(cell);
						break;
					}
					case ColumnBase::intType:{
						Column<int>* col = (Column<int>*) col_b;
						int cell = stoi(temp_col);
						col->updateDictionary(cell);
						break;
					}
					case ColumnBase::llType:{
						// TS
						Column<bigint>* col = (Column<bigint>*) col_b;
						bigint cell = stoll(temp_col);
						col->updateDictionary(cell);
						if(col->getName() == "ts"){
							long cell_lossy = to_lossy(temp_col);
							ts_lossy->updateDictionary(cell_lossy);
						}
						break;
					}
					default:
						Column<string>* col = (Column<string>*) col_b;
						col->updateDictionary(temp_col);
						break;
				}
				t_index++;
			}
			last = next + delim.length();
			c_index++;
		}
	}
	infile.close();
	// update vector value using bulk insert
	for(size_t i = 0; i < columns.size(); i++){
		switch(col_type.at(i)){
			case ColumnBase::uIntType:{
				Column<unsigned int>* col = (Column<unsigned int>*) columns.at(i);
				col->processColumn();
				break;
			}
			case ColumnBase::intType:{
				Column<int>* col2 = (Column<int>*) columns.at(i);
				col2->processColumn();
				break;
			}
			case ColumnBase::llType:{
				Column<bigint>* col3 = (Column<bigint>*) columns.at(i);
				col3->processColumn();
				if(col3->getName() == "ts"){
					ts_lossy->rebuildVecValue();
				}
				break;
			}
			default:
				Column<string>* col4 = (Column<string>*) columns.at(i);
				col4->processColumn();
				break;
		}
	}
	t2 = clock();

	cout << "Loading time: " << ((float)t2-(float)t1) / CLOCKS_PER_SEC  << "s" << endl;
	cout << "Memory consumption after loading: " << getMemory() << "kb" << endl;

	t1 = clock();
	// perform search SID = 40
	vector<size_t> result;
	ColumnBase* col_b = columns.at(0);
	Column<unsigned int>* col = (Column<unsigned int>*) col_b;
	unsigned int input = 40U;
	col->getDictionary()->search(ColumnBase::equal, result, input);
	cout << "Dictionary size of sid: " << col->getDictionary()->size() << " with size: " << col->getDictionary()->getMemoryConsumption()  << endl;
	cout << "Total row of sid = 40: " << result.size() << endl;
	t2 = clock();
	cout << "Running time of sid = 40: " << ((float)t2-(float)t1) / CLOCKS_PER_SEC  << "s"  << endl;
	vector<size_t> output = get_limit(result);
	print_result(output, col);
	cout << "Memory consumption after perform sid = 40: " << getMemory() << "kb" << endl;

//	// V > 5,000,000
	result.clear();
	t1 = clock();
	col_b = columns.at(2);
	col = (Column<unsigned int>*) col_b;
	input = 5000000U;
	col->getDictionary()->search(ColumnBase::lt, result, input);
	cout << "Dictionary size of v : " << col->getDictionary()->size() << " with size: " << col->getDictionary()->getMemoryConsumption() << endl;
	cout << "Total row of V > 5,000,000: " << result.size() << endl;
	t2 = clock();
	cout << "Running time of V > 5,000,000: " << ((float)t2-(float)t1) / CLOCKS_PER_SEC  << "s" << endl;
	output = get_limit(result);
	print_result(output, col);
	cout << "Memory consumption after perform V > 5,000,000: " << getMemory() << "kb" << endl;

	// 120e14 < TS < 121e14
	result.clear();
	t1 = clock();
	col_b = columns.at(1);
	Column<bigint>* ts = (Column<bigint>*) col_b;
	bigint input1 = 1000000000000000LL;
	bigint input2 = 12100000000000000LL;
	cout << "Dictionary size of ts in lossless : " << col->getDictionary()->size() << " with size: " << col->getDictionary()->getMemoryConsumption() << endl;
	cout << "Dictionary size of ts in lossy : " << ts_lossy->getDictionary()->size() << " with size: " << ts_lossy->getDictionary()->getMemoryConsumption() << endl;
	ts->getDictionary()->search(ColumnBase::range, result, input1, input2);
	cout << "Total row of 100e14 < TS < 121e14: " << result.size() << endl;
	t2 = clock();
	cout << "Running time of 100e14 < TS < 121e14: " << ((float)t2-(float)t1) / CLOCKS_PER_SEC  << "s" << endl;
	output = get_limit(result);
	print_result(output, ts);
	cout << "Memory consumption after perform 100e14 < TS < 100e14: " << getMemory() << "kb" << endl;

	// perform search in lossy
	result.clear();
	t1 = clock();
	long lin1 = to_lossy(to_string(input1));
	long lint2 = to_lossy(to_string(input2));
	ts_lossy->getDictionary()->search(ColumnBase::range, result, lin1, lint2);
	cout << "Total row of 100e14 < TS < 121e14 in lossy dict: " << result.size() << endl;
	t2 = clock();
	cout << "Running time of 100e14 < TS < 121e14 in lossy dict: " << ((float)t2-(float)t1) / CLOCKS_PER_SEC  << "s" << endl;
	output = get_limit(result);
	print_result(output, ts_lossy);
	cout << "Memory consumption after perform 100e14 < TS < 100e14 in lossy dict: " << getMemory() << "kb" << endl;
	return 0;
}
