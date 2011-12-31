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
	//rows->tune_options(HashDB::TCOMPRESS);
	rows->open(("data/" + name + ".kct").c_str(), TreeDB::OWRITER | TreeDB::OTRUNCATE | TreeDB::OCREATE);
	rowLen = 0;
	for (int i = 0; i < columns.size(); i ++) {
		columns[i].offset = rowLen;
		rowLen += columns[i].len;
	}
}

void Table::load(const vector<string>& initRows) {
	//cout << "loading " << name << " " << initRows.size() << endl;
	for (int i = 0; i < columns.size(); i ++)
		if (columns[i].needIndex && !columns[i].hasIndex)
			columns[i].initIndex();

	byte* row = NULL;
	vector<vector<pair<int, string> > > parseRet;
	int rowOffset = rows->count();
	for (int i = 0; i < initRows.size(); i ++) {
		int rowNum = rows->count();
		byte kBuf[4];
		getBigNotation(rowNum, kBuf);
		row = parse(initRows[i], parseRet);
		rows->set(kBuf, 4, row, rowLen);
	}

	//cout << "load complete" << endl;

	for (int i = 0; i < columns.size(); i ++) {
		if (!columns[i].needIndex) continue;
		for (int j = 0; j < parseRet.size(); j ++) {
			if (columns[i].type == INT) {
				columns[i].insertIndex(parseRet[j][i].first, rowOffset + j);
			} else {
				columns[i].insertIndex(parseRet[j][i].second, rowOffset + j);
			}
		}
	}

	//cout << "index complete" << endl;
	delete row;
}

void Table::insert(const string& row) {
	int rowNum = rows->count();
	byte kBuf[4];
	getBigNotation(rowNum, kBuf);
	vector<vector<pair<int, string> > > parseRet;
	byte* wBuf = parse(row, parseRet);
	rows->set(kBuf, 4, wBuf, rowLen);

	for (int i = 0; i < columns.size(); i ++) {
		if (!columns[i].needIndex) continue;
		if (columns[i].type == INT) 
			columns[i].insertIndex(parseRet[0][i].first, rowNum);
		else
			columns[i].insertIndex(parseRet[0][i].second, rowNum);
	}
	delete wBuf;
}

byte* Table::parse(const string& s, vector<vector<pair<int, string> > >& parseRet) {
	byte* ret = new char[rowLen];
	vector<pair<int, string> > tmp;
	for (int i = 0, j = 0; i < columns.size(); i ++) {
	//	cout << "col " << i << " ";
		char buf[COLUMN_MAX_LENGTH];
		int k = 0;
		while (s[j] != ',' && j < s.length()) 
			buf[k ++] = s[j ++];
		j ++;
		buf[k] = '\0';

		if (columns[i].type == INT) {
			unsigned int v;
			sscanf(buf, "%u", &v);
	//		cout << v << endl;
			*((unsigned int*)(ret + columns[i].offset)) = v;
			tmp.push_back(make_pair(v, ""));
		} else {
			memcpy(ret + columns[i].offset, buf, k + 1);
	//		cout << buf << endl;
			tmp.push_back(make_pair(-1, buf));
		}
	}
	parseRet.push_back(tmp);
	return ret;
}


