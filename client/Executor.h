#ifndef EXECUTOR_H
#define EXECUTOR_H

#include "Table.h"
#include "lib_tokenize.h"
#include "sqlparser.h"

class Executor {
public:
	string sql;
	vector<Table> tables;
	map<string, int> tableId;
	map<string, int> columnId;	// the table to which column belong


	void createTables(const string& tableName, const vector<string>& columnName,
			const vector<string>& columnType, const vector<string>& primaryKey);

	void train(const vector<string>& query, const vector<double>& weight);

	void load(const string& tableName, const vector<string>& row);

	void preprocess();

	void execute(const string& sql);

	int next(char* row);

	void close();

private:
	map<int, vector<Cond> > condsForTable;	// filter/range conds for table i
	map<pair<int, int>, pair<string, string> > joinForTables;	// join between tables i, j
	map<int, vector<int> > joinsForTable;	// tables to join with of table i
	set<int> tablesToJoin;	// equal to 'FROM tA, tB, tC...'

	vector<int*> joinRet;
	vector<string> output;

	double loadTime, insertTime, selectTime, outputTime, condsTime;

	void markIndex(const string& columnName);
	void initJoinGraph(const SQLParser& sp);
	void genJoinOrder(vector<pair<int, int> >& order);
	void doJoin(const vector<pair<int, int> >& order);
	void genOutput(const vector<string>& cols, const vector<pair<int, int> >& order);
	void sort(vector<int*>& ret, int t, int l, int r);
};

#endif // EXECUTOR_H
