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
	for (int i = 0; i < initRows.size(); i ++) {
		byte* row = parse(initRows[i]);
		rows->set((byte*)&i, 4, row, rowLen);
	}
}

byte* Table::parse(const string& s) {
	byte* ret = new char[rowLen];
	for (int i = 0, j = 0; i < columns.size(); i ++) {
		char buf[COLUMN_MAX_LENGTH];
		int k = 0;
		while (s[j] != ',' && j < s.length()) {
			if (s[j] != '\'')
				buf[k ++] = s[j];
			j ++;
		}
		j ++;
		buf[k] = '\0';

		if (columns[i].type == INT) {
			int v;
			sscanf(buf, "%d", &v);
			*(ret + columns[i].offset) = v;
			//printf("%d,", *(ret + columns[i].offset));
		} else {
			memcpy(ret + columns[i].offset, buf, k + 1);
			//printf("%s,", ret + columns[i].offset);
		}
		//printf("%s", buf);
	}
	//printf("\n");
	return ret;
}


