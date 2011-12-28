#include "Table.h"

Table::Table(const string& name, const vector<string>& columnName,
		const vector<string>& columnType, const vector<string>& primaryKey) {
	this.name = name;
	for (int i = 0; i < columnName.size(); i ++) {
		Column col(columnName[i], columnType[i]);
		columns.push_back(col);
		for (int j = 0; j < primaryKey.size(); j ++) {
			if (primaryKey[j] == columnName[i])
				primary.push_back(i);
		}
	}
	rows = new HashDB();
	rows->open((name + ".kch").c_str(), HashDB::OWRITER | HashDB::OCREATE);
	totRows = 0;
}

