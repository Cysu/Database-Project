#include "Executor.h"

void Executor::createTables(const string& tableName, const vector<string>& columnName, const vector<string>& columnType, const vector<string>& primaryKey) {
	logSwitch = false;
	debugLog("begin: create table " + tableName);
	Table table(tableName, columnName, columnType, primaryKey);
	tables.push_back(table);
	tableId[tableName] = tables.size() - 1;
	for (int i = 0; i < columnName.size(); i ++)
		columnId[columnName[i]] = tables.size() - 1;
	debugLog("end: create table " + tableName);
	debugLog("");

	loadTime = 0;
	insertTime = 0;
	selectTime = 0;
	condsTime = 0;
	outputTime = 0;
}

void Executor::train(const vector<string>& query, const vector<double>& weight) {
	debugLog("begin: train");
	for (int i = 0; i < query.size(); i ++) {
		SQLParser sp(query[i]);
		for (int j = 0; j < sp.join.size(); j ++) {
			markIndex(sp.join[j].colA);
			markIndex(sp.join[j].colB);
		}
		for (int j = 0; j < sp.filter.size(); j ++) {
			markIndex(sp.filter[j].colName);
		}
		for (int j = 0; j < sp.range.size(); j ++)
			markIndex(sp.range[j].colName);
	}
	debugLog("end: train");
	debugLog("");
}

void Executor::preprocess() {
}

void Executor::load(const string& tableName, const vector<string>& row) {
	/*timespec t1, t2;
	clock_gettime(CLOCK_REALTIME, &t1);*/
	debugLog("begin: load table " + tableName);

	int t = tableId[tableName];
	tables[t].load(row);

	debugLog("end: load table " + tableName);
	debugLog("");
	/*clock_gettime(CLOCK_REALTIME, &t2);
	loadTime += t2.tv_sec - t1.tv_sec + 1.0 * (t2.tv_nsec - t1.tv_nsec) / 1000000000.0;*/
}

void Executor::execute(const string& sql) {
	debugLog("begin: execute sql " + sql);
	// execute INSERT
	if (strstr(sql.c_str(), "INSERT") != NULL) {
		/*timespec t1, t2;
		clock_gettime(CLOCK_REALTIME, &t1);*/

		vector<string> token;
		lib_tokenize(sql.c_str(), token);
		Table& t = tables[tableId[token[2]]];
		for (int i = 4; i < token.size(); i ++) {
			if (token[i] == ";" || token[i] == "," || token[i] == "(" || token[i] == ")") continue;
			t.insert(token[i].substr(1, token[i].length() - 2));
		}

		debugLog("");
		/*clock_gettime(CLOCK_REALTIME, &t2);
		insertTime += t2.tv_sec - t1.tv_sec + 1.0 * (t2.tv_nsec - t1.tv_nsec) / 1000000000.0;*/
		return;
	}

	// execute SELECT
	/*timespec t1, t2;
	clock_gettime(CLOCK_REALTIME, &t1);*/

	SQLParser sp(sql);
	initJoinGraph(sp);

	vector<pair<int, int> > order;
	genJoinOrder(order);

	doJoin(order);
	
	/*timespec t3, t4;
	clock_gettime(CLOCK_REALTIME, &t3);*/
	genOutput(sp.output, order);
	/*clock_gettime(CLOCK_REALTIME, &t4);
	outputTime += t4.tv_sec - t3.tv_sec + 1.0 * (t4.tv_nsec - t3.tv_nsec) / 1000000000.0;*/

	debugLog("");
	/*clock_gettime(CLOCK_REALTIME, &t2);
	selectTime += t2.tv_sec - t1.tv_sec + 1.0 * (t2.tv_nsec - t1.tv_nsec) / 1000000000.0;*/
}

int Executor::next(char* row) {
	if (output.empty()) return 0;
	strcpy(row, output.back().c_str());
	output.pop_back();
	return 1;
}

void Executor::close() {
	/*printf("loadTime = %lf\n", loadTime);
	printf("insertTime = %lf\n", insertTime);
	printf("selectTime = %lf\n", selectTime);
	printf("\tcondsTime = %lf\n", condsTime);
	printf("outputTime = %lf\n", outputTime);*/
}


void Executor::markIndex(const string& columnName) {
	int t = columnId[columnName];
	int c = tables[t].getColumnId(columnName);
	tables[t].columns[c].needIndex = true;
}

void Executor::initJoinGraph(const SQLParser& sp) {
	condsForTable.clear();
	joinForTables.clear();
	joinsForTable.clear();
	tablesToJoin.clear();

	joinRet.clear();

	for (int i = 0; i < sp.filter.size(); i ++) {
		int t = columnId[sp.filter[i].colName];
		condsForTable[t].push_back(sp.filter[i]);
		tablesToJoin.insert(t);
	}
	for (int i = 0; i < sp.range.size(); i ++) {
		int t = columnId[sp.range[i].colName];
		condsForTable[t].push_back(sp.range[i]);
		tablesToJoin.insert(t);
	}

	for (int i = 0; i < sp.join.size(); i ++) {
		string cA = sp.join[i].colA;
		string cB = sp.join[i].colB;
		int tA = columnId[cA];
		int tB = columnId[cB];
		joinForTables[make_pair(tA, tB)] = make_pair(cA, cB);
		joinForTables[make_pair(tB, tA)] = make_pair(cB, cA);
		joinsForTable[tA].push_back(tB);
		joinsForTable[tB].push_back(tA);
		tablesToJoin.insert(tA);
		tablesToJoin.insert(tB);
	}
}

void Executor::genJoinOrder(vector<pair<int, int> >& order) {
	int t = *(tablesToJoin.begin());

	bool mark[MAX_TABLES];	
	memset(mark, false, sizeof(mark));
	mark[t] = true;

	int head = 0;
	while (order.size() < tablesToJoin.size() - 1) {
		for (int i = 0; i < joinsForTable[t].size(); i ++) {
			int u = joinsForTable[t][i];
			if (!mark[u]) {
				mark[u] = true;
				order.push_back(make_pair(t, u));
			}
		}
		t = order[head].second;
		head ++;
	}
	if (order.size() == 0)
		order.push_back(make_pair(t, -1));
}

void Executor::doJoin(const vector<pair<int, int> >& order) {
	set<int> condsRet;
	// the first table
	int t = order[0].first;

	/*timespec t1, t2;
	clock_gettime(CLOCK_REALTIME, &t1);*/

	tables[t].getByConds(condsForTable[t], condsRet);

	/*clock_gettime(CLOCK_REALTIME, &t2);
	condsTime += t2.tv_sec - t1.tv_sec + 1.0 * (t2.tv_nsec - t1.tv_nsec) / 1000000000.0;*/

	if (condsRet.size() == 0) {
		for (int i = 0; i < tables[t].rows->count(); i ++) {
			int* joinRetItem = new int[tablesToJoin.size()];
			joinRetItem[0] = i;
			joinRet.push_back(joinRetItem);
		}
	} else if (condsRet.find(-1) == condsRet.end()) {
		for (set<int>::iterator it = condsRet.begin(); it != condsRet.end(); it ++) {
			int* joinRetItem = new int[tablesToJoin.size()];
			joinRetItem[0] = *it;
			joinRet.push_back(joinRetItem);
		}
	}

	if (order[0].second == -1) return;

	for (int i = 0; i < order.size(); i ++) {
		int tA = order[i].first, tB = order[i].second;
		int cA = tables[tA].getColumnId(joinForTables[make_pair(tA, tB)].first);
		int cB = tables[tB].getColumnId(joinForTables[make_pair(tA, tB)].second);

		condsRet.clear();

		/*clock_gettime(CLOCK_REALTIME, &t1);*/
		tables[tB].getByConds(condsForTable[tB], condsRet);
		/*clock_gettime(CLOCK_REALTIME, &t2);
		condsTime += t2.tv_sec - t1.tv_sec + 1.0 * (t2.tv_nsec - t1.tv_nsec) / 1000000000.0;*/

		sort(joinRet, i, 0, joinRet.size() - 1);

		vector<int*> newJoinRet;
		vector<int> matchRows;
		for (int j = 0; j < joinRet.size(); j ++) {
			if (j > 0 && joinRet[j][i] == joinRet[j - 1][i]) continue;
			matchRows.clear();
			if (tables[tA].columns[cA].type == INT) {
				unsigned int v = tables[tA].getIntValue(joinRet[j][i], cA);
				tables[tB].columns[cB].filterBy(v, EQU, matchRows);
			} else {
				string v = tables[tA].getStringValue(joinRet[j][i], cA);
				tables[tB].columns[cB].filterBy(v, matchRows);
			}
			for (int k = 0; k < matchRows.size(); k ++) {
				if (condsRet.size() == 0 || condsRet.find(matchRows[k]) != condsRet.end()) {
					int* joinRetItem = new int[tablesToJoin.size()];
					memcpy(joinRetItem, joinRet[j], tablesToJoin.size() * 4);
					joinRetItem[i + 1] = matchRows[k];
					newJoinRet.push_back(joinRetItem);
				}
			}
		}
		for (int j = 0; j < joinRet.size(); j ++)
			delete joinRet[j];
		joinRet = newJoinRet;
	}
}

void Executor::genOutput(const vector<string>& cols, const vector<pair<int, int> >& order) {
	output.clear();
	map<int, int> orderOfTable;
	orderOfTable[order[0].first] = 0;
	if (order[0].second != -1) {
		for (int i = 0; i < order.size(); i ++)
			orderOfTable[order[i].second] = i + 1;
	}

	for (int i = 0; i < joinRet.size(); i ++) {
		string buf;
		for (int j = 0; j < cols.size(); j ++) {
			int t = columnId[cols[j]];
			int o = orderOfTable[t];
			int r = joinRet[i][o];
			int c = tables[t].getColumnId(cols[j]);
			if (tables[t].columns[c].type == INT) {
				unsigned int v = tables[t].getIntValue(r, c);
				char tmp[20];
				sprintf(tmp, "%u", v);
				buf += tmp;
			} else {
				string v = tables[t].getStringValue(r, c);
				buf += v;
			}
			if (j < cols.size() - 1)
				buf += ",";
		}
		output.push_back(buf);
	}
}

void Executor::sort(vector<int*>& ret, int t, int l, int r) {
	if (l >= r) return;
	int i = l, j = r, x = ret[(l + r) >> 1][t];
	while (i <= j) {
		while (ret[i][t] < x) i ++;
		while (ret[j][t] > x) j --;
		if (i <= j) {
			int* tmp = ret[i];
			ret[i] = ret[j];
			ret[j] = tmp;
			i ++;
			j --;
		}
	}
	if (i < r) sort(ret, t, i, r);
	if (l < j) sort(ret, t, l, j);
}

