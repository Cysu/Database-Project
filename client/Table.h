#ifndef TABLE_H
#define TABLE_H

#include <string>
#include <vector>
#include <cstring>

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
};


#endif // TABLE_H
