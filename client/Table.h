#ifndef TABLE_H
#define TABLE_H

#include <string>
#include <vector>
#include <cstring>
#include "Column.h"

using namespace std;

class Table {
public:
	string name;
	vector<Column> columns;
	HashDB* rows;
	int totRows;
	vector<int> primary;

	Table(const string& name, const vector<string>& columnName,
			const vector<string>& columnType, const vector<string>& primaryKey);

	void load(const vector<string>& initRows);

private:
	int rowLen;	// length of each row(in bytes)
	byte* parse(const string& s, int rowNum);
};


#endif // TABLE_H
