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
	vector<int> primary;

	Table(const string& name, const vector<string>& columnName,
			const vector<string>& columnType, const vector<string>& primaryKey);

	void load(const vector<string>& initRows);
	void insert(const string& row);

private:
	int rowLen;	// length of each row(in bytes)
	byte* parse(const string& s, vector<vector<pair<int, string> > >& parseRet);
};


#endif // TABLE_H
