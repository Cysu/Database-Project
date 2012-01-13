#include "Table.h"

Table::Table(const string& name, const vector<string>& columnName,
		const vector<string>& columnType, const vector<string>& primaryKey) {
	this->name = name;

	// make columns
	// ignore primary
	for (int i = 0; i < columnName.size(); i ++) {
		Column col(columnName[i], columnType[i]);
		columns.push_back(col);
		columnId[columnName[i]] = i;
	}

	// make rows
	rows = new HashDB();
	rows->open(("data/"+name+".kct").c_str(), TreeDB::OWRITER | TreeDB::OTRUNCATE | TreeDB::OCREATE);

	rowLen = 0;
	for (int i = 0; i < columns.size(); i ++) {
		columns[i].offset = rowLen;
		rowLen += columns[i].len;
	}
}

int Table::getColumnId(string columnName) {
	return columnId.find(columnName)->second;
}

void Table::load(const vector<string>& initRows) {
	for (int i = 0; i < initRows.size(); i ++)
		insert(initRows[i]);
}

void Table::insert(const string& insertRow) {
	int rowNum = rows->count();
	vector<pair<int, string> > parseRet;
	byte* content = parse(insertRow, parseRet);
	rows->set((byte*)&rowNum, 4, content, rowLen);

	// make index
	for (int j = 0; j < columns.size(); j ++) {
		if (!columns[j].needIndex) continue;
		if (columns[j].type == INT)
			columns[j].insertIndex(parseRet[j].first, rowNum);
		else
			columns[j].insertIndex(parseRet[j].second, rowNum);
	}
	delete content;
}

unsigned int Table::getIntValue(int r, int c) {
	size_t tmp;
	byte* content = rows->get((byte*)&r, 4, &tmp);
	unsigned int t = *((unsigned int*)(content + columns[c].offset));
	delete content;
	return t;
}

string Table::getStringValue(int r, int c) {
	size_t tmp;
	byte* content = rows->get((byte*)&r, 4, &tmp);
	string t = content + columns[c].offset;
	delete content;
	return t;
}
	
byte* Table::parse(const string& s, vector<pair<int, string> >& parseRet) {
	byte* ret = new byte[rowLen];
	for (int i = 0, j = 0; i < columns.size(); i ++) {
		byte buf[COLUMN_MAX_LENGTH];
		int k = 0;
		while (s[j] != ',' && j < s.length())
			buf[k ++] = s[j ++];
		j ++;
		buf[k] = '\0';
		
		if (columns[i].type == INT) {
			unsigned int v;
			sscanf(buf, "%u", &v);
			*((unsigned int*)(ret + columns[i].offset)) = v;
			parseRet.push_back(make_pair(v, ""));
		} else {
			memcpy(ret + columns[i].offset, buf, k + 1);
			parseRet.push_back(make_pair(-1, buf));
		}
	}
	return ret;
}

void Table::getByConds(vector<Cond>& conds, set<int>& ret) {
	if (conds.size() == 0) return;

	// sort the conds
	for (int i = 0; i < conds.size(); i ++)
		for (int j = i + 1; j < conds.size(); j ++) {
			int iCount = condHeuristic(conds[i]);
			int jCount = condHeuristic(conds[j]);
			if (iCount > jCount) {
				Cond tmp = conds[i];
				conds[i] = conds[j];
				conds[j] = tmp;
			}
		}

	// get a initial result
	int cId = getColumnId(conds[0].colName);
	vector<int> initRet;
	switch (conds[0].type) {
		OPR_TYPE type;
		case IFIL:
			columns[cId].filterBy(conds[0].c_int, EQU, initRet);
			break;
		case SFIL:
			columns[cId].filterBy(conds[0].c_string, initRet);
			break;
		case RANG:
			type = conds[0].op == '<' ? LES : GTR;
			columns[cId].filterBy(conds[0].c_int, type, initRet);
			break;
		default:
			break;
	}

	// check if the row satisfy all the conds
	for (int i = 0; i < initRet.size(); i ++) {
		bool match = true;
		int r = initRet[i];
		for (int j = 1; j < conds.size(); j ++) {
			int c = getColumnId(conds[j].colName);
			int iValue;
			string sValue;
			if (columns[c].type == INT)
				iValue = getIntValue(r, c);
			else
				sValue = getStringValue(r, c);

			switch (conds[j].type) {
				case IFIL:
					if (iValue != conds[j].c_int)
						match = false;
					break;
				case SFIL:
					if (sValue != conds[j].c_string)
						match = false;
					break;
				case RANG:
					if ((conds[j].op == '<' && iValue >= conds[j].c_int) ||
							(conds[j].op == '>' && iValue <= conds[j].c_int))
						match = false;
					break;
				default:
					break;
			}
			if (!match) break;
		}
		
		if (match) ret.insert(r);
	}

	// no row
	if (ret.size() == 0)
		ret.insert(-1);
}

int Table::condHeuristic(const Cond& cond) {
	int c = getColumnId(cond.colName);
	if (cond.type == IFIL)
		return columns[c].getIndexCount(EQU, cond.c_int);
	else if (cond.type == SFIL)
		return columns[c].getIndexCount(cond.c_string);
	else if (cond.type == RANG) {
		if (cond.op == '<')
			return columns[c].getIndexCount(LES, cond.c_int);
		else
			return columns[c].getIndexCount(GTR, cond.c_int);
	}
}
