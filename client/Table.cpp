#include "Table.h"

Table::Table(const string& name, const vector<string>& columnName,
		const vector<string>& columnType, const vector<string>& primaryKey) {
	this->name = name;

	// make columns
	for (int i = 0; i < columnName.size(); i ++) {
		Column col(columnName[i], columnType[i]);
		columns.push_back(col);
		for (int j = 0; j < primaryKey.size(); j ++) {
			if (primaryKey[j] == columnName[i])
				primary.push_back(i);
		}
	}

	// make rows
	rows = new HashDB();
	rows->open(("data/" + name + ".kch").c_str(), HashDB::OWRITER | HashDB::OCREATE);
	totRows = 0;
	rowLen = 0;
	for (int i = 0; i < columns.size(); i ++) {
		columns[i].offset = rowLen;
		rowLen += columns[i].len;
	}
}

void Table::load(const vector<string>& initRows) {
	for (int i = 0; i < columns.size(); i ++)
		if (columns[i].needIndex)
			columns[i].initIndex();

	for (int i = 0; i < initRows.size(); i ++) {
		byte* row = parse(initRows[i], i);
		rows->set((byte*) &i, 4, row, rowLen);
	}

	totRows = initRows.size();
}

void Table::insert(const string& row) {
	byte* wBuf = parse(row, totRows);
	rows->set((byte*) &totRows, 4, wBuf, rowLen);
	totRows++;
}

byte* Table::parse(const string& s, int rowNum) {
	byte* ret = new char[rowLen];
	for (int i = 0, j = 0; i < columns.size(); i ++) {
		char buf[COLUMN_MAX_LENGTH];
		int k = 0;
		while (s[j] != ',' && j < s.length()) 
			buf[k ++] = s[j ++];
		j ++;
		buf[k] = '\0';

		if (columns[i].type == INT) {
			int v;
			sscanf(buf, "%d", &v);
			*(ret + columns[i].offset) = v;
			// insertIndex
			if (columns[i].needIndex)
				columns[i].insertIndex(v, rowNum);
		} else {
			memcpy(ret + columns[i].offset, buf, k + 1);
			// insertIndex
			if (columns[i].needIndex)
				columns[i].insertIndex(buf, rowNum);
		}
	}
	return ret;
}


