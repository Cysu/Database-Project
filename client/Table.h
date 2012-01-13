#ifndef TABLE_H
#define TABLE_H

#include <set>
#include "Column.h"
#include "cond.h"
#include "kchashdb.h"

using namespace kyotocabinet;

class Table {
public:
	string name;
	vector<Column> columns;
	map<string, int> columnId;
	HashDB* rows;
	int rowLen;

	Table(const string& name, const vector<string>& columnName,
			const vector<string>& columnType, const vector<string>& primaryKey);

	int getColumnId(string columnName);

	void load(const vector<string>& initRows);
	void insert(const string& insertRow);

	unsigned int getIntValue(int r, int c);
	string getStringValue(int r, int c);

	void getByConds(vector<Cond>& conds, set<int>& ret);

	int condHeuristic(const Cond& cond);

private:
	byte* parse(const string& s, vector<pair<int, string> >& parseRet);
};

#endif // TABLE_H
