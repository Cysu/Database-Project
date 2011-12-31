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
	rows = new TreeDB();
	rows->open(("data/" + name + ".kct").c_str(), TreeDB::OWRITER | TreeDB::OCREATE | TreeDB::ONOLOCK);
	rowLen = 0;
	for (int i = 0; i < columns.size(); i ++) {
		columns[i].offset = rowLen;
		rowLen += columns[i].len;
	}
}

void Table::load(const vector<string>& initRows) {
	for (int i = 0; i < columns.size(); i ++)
		if (columns[i].needIndex && !columns[i].hasIndex)
			columns[i].initIndex();

	byte* row = NULL;
	for (int i = 0; i < initRows.size(); i ++) {
		int rowNum = rows->count();
		row = parse(initRows[i], rowNum);
		rows->set((byte*) &rowNum, 4, row, rowLen);
	}
	delete row;
}

void Table::insert(const string& row) {
	int rowNum = rows->count();
	byte* wBuf = parse(row, rowNum);
	rows->set((byte*) &rowNum, 4, wBuf, rowLen);
	delete wBuf;
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
			unsigned int v;
			sscanf(buf, "%u", &v);
			*((unsigned int*)(ret + columns[i].offset)) = v;
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


