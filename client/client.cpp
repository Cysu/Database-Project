#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <map>
#include <cassert>
#include <iostream>
#include <utility>

#include "../include/client.h"
#include "lib_tokenize.h"
#include "Table.h"
#include "sqlparser.h"
#include "cond.h"
#include "JoinAgent.h"

using namespace std;
using namespace __gnu_cxx;

vector<Table> tables;
map<string, int> tableId;
map<string, pair<int, int> > columnId;
vector<string> result;

int filter(Table& t, const set <Cond>& FCond); 
void getJoinOrder(int*);

//maps table id to the col ids that used for join or project
//i.e. the col that need to be read from The BIG TABLE
map <int, set <int> > mttcsJoin, mttcsProj;

//map table id to the Conds that need for filter
map <int, set <Cond> >  mttFCond/*, mttRCond*/;

//map col to Join Cond
map <pair<int, int>, set <Cond> > mctCondJoin;

//set of Join Cond, if it is empty the query is done
set <Cond> JConds;

//map table id to the expected row# after filter
map <int, int> mttRowNum;

//the cols that need to be output
vector <pair <int, int> > outputCols;

//the joinOrder
int* joinOrder;

//internal result
vector <int*> jaRet;

hash_set<int> s[2];

void initJoinGraph(SQLParser& sp);
void genJoinOrder(SQLParser& sp, int* joinOrder);
void genOutput(int start, int len);
int resultRowNum, outputRowNum;


void create(const string& table_name, const vector<string>& column_name,
	const vector<string>& column_type, const vector<string>& primary_key)
{
	Table table(table_name, column_name, column_type, primary_key);
	tables.push_back(table);
	tableId[table_name] = tables.size() - 1;
	for (int i = 0; i < column_name.size(); i ++)
		columnId[column_name[i]] = make_pair(tables.size() - 1, i);
}

void train(const vector<string>& query, const vector<double>& weight)
{
	pair<int, int> tmp;
	for (int i = 0; i < query.size(); i++) {
		SQLParser sp(query[i]);
		for (int j = 0; j < sp.join.size(); j++) {

			// mark to index
			tmp = columnId[sp.join[j].colA];
			tables[tmp.first].columns[tmp.second].needIndex = true;
			tmp = columnId[sp.join[j].colB];
			tables[tmp.first].columns[tmp.second].needIndex = true;
		}
		for (int j = 0; j < sp.filter.size(); j++) {

			tmp = columnId[sp.filter[j].colName];
			tables[tmp.first].columns[tmp.second].needIndex = true;
		}
		for (int j = 0; j < sp.range.size(); j++) {

			tmp = columnId[sp.range[j].colName];
			tables[tmp.first].columns[tmp.second].needIndex = true;
		}	
	}
}

void load(const string& table_name, const vector<string>& row)
{
	tables[tableId[table_name]].load(row);
}

void preprocess()
{
}

void execute(const string& sql)
{
	vector<string> token, table, row;
	map<string, int> m;
	int i;
	result.clear();
	//cout << sql << endl;
	if (strstr(sql.c_str(), "INSERT") != NULL) {
		lib_tokenize(sql.c_str(), token);
		i += 2;//skip INSERT INTO
		Table& t = tables[tableId[token[i++]]];	
		for (i++; i < token.size(); i++) {
			if (token[i] == ";" || token[i] == ",")
				continue;
			t.insert(token[i].substr(1, token[i].size() - 2));	
		}
		return;	
	}

	//SELECT
	//cout << "querying... " << sql << endl;
	SQLParser sp(sql);
	//cout << "SQLParser ok" << endl;
	//cout << "join number:" << sp.join.size() << endl;
	//cout << "table number:" << sp.tables.size() << endl;
	initJoinGraph(sp);
	joinOrder = new int[JConds.size()*4];	// delete at the end of next()
	genJoinOrder(sp, joinOrder);

	/*for (int i = 0; i < JConds.size(); i ++) {
		printf("%d %d %d %d\n", joinOrder[i*4], joinOrder[i*4+1], joinOrder[i*4+2], joinOrder[i*4+3]);
	}*/

	int f;
	JoinAgent ja(tables, JConds.size() + 1, joinOrder);
	if (JConds.size() > 0) {
		f = filter(tables[joinOrder[0]], mttFCond[joinOrder[0]]);
	} else {
		f = filter(tables[tableId[sp.tables[0]]], mttFCond[tableId[sp.tables[0]]]);
	}


	ja.init(s[f]);
	//ja.output(ja.ret);
	for (int i = 0; i < JConds.size(); i ++) {
		f = filter(tables[joinOrder[i * 4 + 2]], mttFCond[joinOrder[i * 4 + 2]]);
	//	cout << "internal Result size:" << ja.ret.size() << " next table size:" << s[f].size() << endl;
		ja.join(i, s[f]);
	}
	jaRet = ja.ret;	
	//ja.output(ja.ret);
	//cout << ja.ret.size() << endl;

	if (JConds.size() <= 0) {
		joinOrder = new int[1];
		joinOrder[0] = tableId[sp.tables[0]];
	}
	outputRowNum = 0;
	//genOutput(0, BLOCK_SIZE);
}


int next(char *row)
{
	if (result.size() <= 0 /*|| outputRowNum > 1*/){
		if (outputRowNum == jaRet.size() /*|| outputRowNum > 1*/) {
			for (int i = 0; i < jaRet.size(); i++)
				delete jaRet[i];
			jaRet.clear();
			delete joinOrder;
			return (0);
		} else
			genOutput(outputRowNum, BLOCK_SIZE);
	}
	strcpy(row, result.back().c_str());
//	printf("%s\n", row);
	result.pop_back();
	outputRowNum++;

	return (1);
}

void close()
{
}

void initJoinGraph(SQLParser& sp) {
	mttRowNum.clear();
	mttcsJoin.clear();
	mctCondJoin.clear();
	mttFCond.clear();
	mttcsProj.clear();
	JConds.clear();
	outputCols.clear();
	//cout << "start" << endl;
	for (int i = 0; i < tables.size(); i++) {
		mttRowNum[i] = tables[i].rows->count();
	}	
	//cout << "init mttRowNum" << endl;
	for (int i = 0; i < sp.output.size(); i++) {
		int tid = columnId[sp.output[i]].first;
		int cid = columnId[sp.output[i]].second;
		mttcsProj[tid].insert(cid);
		outputCols.push_back(make_pair(tid, cid));	
	}
	//cout << "project cols ok" << endl;
	for (int i = 0; i < sp.join.size(); i++) {
		int tid = columnId[sp.join[i].colA].first;
		int cid = columnId[sp.join[i].colA].second;
		mttcsJoin[tid].insert(cid);
		mctCondJoin[make_pair(tid, cid)].insert(sp.join[i]);

		tid = columnId[sp.join[i].colB].first;
		cid = columnId[sp.join[i].colB].second;
		mttcsJoin[tid].insert(cid);
		mctCondJoin[make_pair(tid, cid)].insert(sp.join[i]);
		JConds.insert(sp.join[i]);
		
	}
	//cout << "join cols ok" << endl;
	//cout << JConds.size() << endl;
	for (int i = 0; i < sp.filter.size(); i++) {
		int tid = columnId[sp.filter[i].colName].first;
		int cid = columnId[sp.filter[i].colName].second;
		mttFCond[tid].insert(sp.filter[i]);	
		mttRowNum[tid] = mttRowNum[tid]/tables[tid].columns[cid].index->count();
	}
	//cout << "filter cols ok" << endl;
	for (int i = 0; i < sp.range.size(); i++) {
		int tid = columnId[sp.range[i].colName].first;
		mttFCond[tid].insert(sp.range[i]);	
		mttRowNum[tid] = mttRowNum[tid]/3;
	}
	//cout << "range cols ok" << endl;
}

void genJoinOrder(SQLParser&sp, int* joinOrder) {
	//cout << "gen join oreder" << endl;
	int* q = joinOrder;
	int min = 2147483647;
	int curTable;
	for (int i = 0; i < sp.tables.size(); i++) {
		//cout << sp.tables[i] << endl;
		if (mttRowNum[tableId[sp.tables[i]]] < min) {
			//cout << "tableId:" << tableId[sp.tables[i]] << endl;
			//cout << "numOfRows:" << mttRowNum[tableId[sp.tables[i]]] << endl;
			min = mttRowNum[tableId[sp.tables[i]]];
			curTable = tableId[sp.tables[i]];
			//cout << "curTable:" << curTable << endl;
		}
	}
	set <Cond> curConds, oldConds;	
	set <int> oldTable;
	while (q - joinOrder < JConds.size()*4) {
		if (JConds.empty())
			break;
		oldTable.insert(curTable);
		for (set <int>::iterator it = mttcsJoin[curTable].begin(); it != mttcsJoin[curTable].end(); it++) {
			set <Cond> t = mctCondJoin[make_pair(curTable, (*it))];
			for (set <Cond>::iterator jt = t.begin(); jt != t.end(); jt++)
				if (oldConds.find((*jt)) == oldConds.end()) {
					curConds.insert((*jt));
					//cout << "a:" << jt->colA << ",b:" << jt->colB << endl;
				}
		}			
		Cond toJoin;
		min = 2147483647;
		for (set <Cond>::iterator it = curConds.begin(); it != curConds.end(); it++) {
			int tidA = columnId[it->colA].first;
			int cidA = columnId[it->colA].second;
			int tidB = columnId[it->colB].first;
			int cidB = columnId[it->colB].second;
			int max = tables[tidA].columns[cidA].index->count() > tables[tidB].columns[cidB].index->count() ?
				tables[tidA].columns[cidA].index->count() : tables[tidB].columns[cidB].index->count();
			if (mttRowNum[tidA] * mttRowNum[tidB] / max < min) {
				min = mttRowNum[tidA]*mttRowNum[tidB]/max;
				toJoin = (*it);
				if (oldTable.find(tidA)!=oldTable.end()){
					(*q)=tidA;
					*(q+1)=cidA;
					*(q+2)=tidB;
					*(q+3)=cidB;
					curTable = tidB;
				} else {
					*(q+0)=tidB;
					*(q+1)=cidB;
					*(q+2)=tidA;
					*(q+3)=cidA;
					curTable = tidA;
				}
			}
		}
		q += 4;
		oldConds.insert(toJoin);
		curConds.erase(toJoin);
	}
}

vector <vector <string> > t_result(BLOCK_SIZE);
void genOutput(int start, int len) {
	byte* rowContent = NULL;
	int resLen = jaRet.size() - start < len ? jaRet.size() - start : len;
	result.resize(resLen, string());
	for (int i = 0; i < resLen; i++)
		t_result[i].resize(outputCols.size(), string());	
	//for each tables
	for (int i = 0; i < tables.size(); i++) {
		vector <int> cids, positions;
		//iterate over cols need to be output
		for (set <int>::iterator it = mttcsProj[i].begin(); it != mttcsProj[i].end(); it++) {
			cids.push_back(*it);
			//for each cols find the positon in outputCols
			int pos;
			for (pos = 0; pos < outputCols.size(); pos++)
				if (i == outputCols[pos].first && (*it) == outputCols[pos].second) 
					break;				
			positions.push_back(pos);		
		}	
		//read all the data in cols indicated by cids and store them it t_result cols indicated by positions
		//read at most len rows
		//find the col num in jaRet that store the rowId of table i
		int pos;
		if (joinOrder[0] == i)
			pos = 0;
		else {
			for (pos = 1; pos < JConds.size(); pos++)
				if (joinOrder[pos * 4 - 2] == i)
					break;
		}
		//JoinAgent::sort(jaRet, pos, 0, jaRet.size() - 1);	
		for (int jj = start; jj < jaRet.size() && jj < start + len; jj++) {
			int j = jj - start;
			size_t rowLen;
			byte kBuf[4];
			getBigNotation(jaRet[jj][pos], kBuf);
			rowContent = tables[i].rows->get(kBuf, 4, &rowLen);
			//get the data...
			for (int k = 0; k < cids.size(); k++) {
				int colOffset = tables[i].columns[cids[k]].offset;
				int colLen = tables[i].columns[cids[k]].len;
				if (tables[i].columns[cids[k]].type == INT) {
					unsigned int t = *((unsigned int*)(rowContent + colOffset));
					char buf[20];
					sprintf(buf, "%u", t);
					t_result[j][positions[k]] = string(buf);
				} else {
					string t = rowContent + colOffset;
					t_result[j][positions[k]] = t;
				}	
			}
			
			delete rowContent;
		}
		
	}	
	
	//put t_result int result...
	for (int jj = start; jj < jaRet.size() && jj < start + len; jj++) {
		int j = jj - start;
		for (int k = 0; k < outputCols.size() - 1; k++) {
			result[j] += t_result[j][k] + ",";
		}
		result[j] += t_result[j][outputCols.size() - 1];
	}		
	/*
	for (int i = 0; i < sp.output.size(); i++) {
		int tid = columnId[sp.output[i]].first;
		int cid = columnId[sp.output[i]].second;
		int rid;
		for (rid = 0; rid < JConds.size(); rid++)
			if (joinOrder[rid * 4 + 2] == tid)
				break;
		if (tid == joinOrder[JConds.size() * 4 - 2])
			rid = JConds.size();
		ja.sort(rid, 0, ja.ret.size()-1);
		for (int j = 0; j < ja.ret.size(); j++) {
			size_t rowLen;
			rowContent = tables[tid].rows->get((byte*)&(ja.ret[j][rid]), 4, &rowLen);
			int colOffset = tables[tid].columns[cid].offset;
			int colLen = tables[tid].columns[cid].len;
			if (tables[tid].columns[cid].type == INT) {
				unsigned int t = *(rowContent + colOffset);
				char buf[20];
				sprintf(buf, "%u", t);
				result[j] += string(buf) + ",";
			} else {
				string t = rowContent + colOffset;
				result[j] += t + ",";
			}
		}
	}
	*/
}

int filter(Table& t, const set <Cond>& FCond) {
	//cout << "filter" << endl;
	s[0].clear();
	vector<int> temp;
	int p = 0;
	if (FCond.empty()) {
		//cout << "no filter, size:" << s[0].size() << endl;
		return 0;
	}
	int max;
	set <Cond>::iterator indexCondi = FCond.begin();
	// find the best cond
	if (FCond.begin()->type == RANG)
		max = 3;
	else
		max = t.columns[columnId[FCond.begin()->colName].second].index->count();
	for (set <Cond>::iterator it = FCond.begin(); it != FCond.end(); it++) {
		if (it->type == RANG) {
			if (3 > max) {
				max = 3;
				indexCondi = it;
			}
		} else {
			if (t.columns[columnId[it->colName].second].index->count() > max) {
				max = t.columns[columnId[it->colName].second].index->count();
				indexCondi = it;
			}
		}
	}
	//cout << indexCondi->colName << endl;
	// filter by indexCond
	int cid = columnId[indexCondi->colName].second;
	switch(indexCondi->type) {
		case IFIL:
			//cout << "IFIL" << endl;
			t.columns[cid].filterBy(indexCondi->c_int, EQU, temp);
			break;
		case SFIL:
			t.columns[cid].filterBy(indexCondi->c_string, temp);
			break;
		case RANG:
			switch(indexCondi->op) {
				case '<':
					t.columns[cid].filterBy(indexCondi->c_int, LES, temp);
					break;
				case '>':
					t.columns[cid].filterBy(indexCondi->c_int, GTR, temp);
					break;
			}
			break;
	}
	//cout << "temp size:" << temp.size() << endl;
	if (temp.size() == 0) {
		s[0].insert(-1);
		//cout << "no result size:" << s[0].size() << endl;
		return 0;
	}
	// filter by the remain Cond
	if (FCond.size() == 1) {
		for (int i = 0; i < temp.size(); i++)
			s[0].insert(temp[i]);
		//cout << "only one filter, set size:" << s[0].size() << endl;
		return 0;
	}
	byte* rowContent = NULL;
	for (int i = 0; i < temp.size(); i++) {
		size_t rowLen;
		byte kBuf[4];
		getBigNotation(temp[i], kBuf);
		rowContent = t.rows->get(kBuf, 4, &rowLen);
		bool match = true;
		for (set <Cond>::iterator it = FCond.begin(); it != FCond.end(); it++) {
			if (it == indexCondi)
				continue;
			int colOffset = t.columns[columnId[it->colName].second].offset;
			unsigned int integer;
			string str;
			switch (it->type) {
				case SFIL:
					str = rowContent + colOffset;
					if (str != it->c_string)
						match = false;
					break;
				case IFIL:
					integer = *((unsigned int*)(rowContent + colOffset));
					if (integer != it->c_int)
						match = false;
					break;
				case RANG:
					integer = *((unsigned int*)(rowContent + colOffset));
					if (it->op == '<' && integer >= it->c_int)
						match = false;
					if (it->op == '>' && integer <= it->c_int)
						match = false;
					break;
			}
			if (!match)
				break;
		}
		if (match)
			s[0].insert(temp[i]);
		delete rowContent;
	}
	//cout << "filter by other filter, set size:" << s[0].size() << endl;
	if (s[0].size() == 0)
		s[0].insert(-1);
	return 0;
}	

