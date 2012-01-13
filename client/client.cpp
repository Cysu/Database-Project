#include "Executor.h"

vector<Table> tables;
map<string, int> tableId;
map<string, int> columnId;	// the table to which column belong

Executor e;

void create(const string& tableName, const vector<string>& columnName,
		const vector<string>& columnType, const vector<string>& primaryKey) {
	e.createTables(tableName, columnName, columnType, primaryKey);
}

void train(const vector<string>& query, const vector<double>& weight) {
	e.train(query, weight);
}

void load(const string& tableName, const vector<string>& row) {
	e.load(tableName, row);
}

void preprocess() {
	e.preprocess();
}

void execute(const string& sql) {
	e.execute(sql);
}

int next(char* row) {
	return e.next(row);
}

void close() {
	e.close();
}
	
