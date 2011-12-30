#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <map>
#include <cassert>
#include <iostream>
#include <utility>

#include "../include/client.h"
#include "../lib/tokenize.h"
#include "../lib/split_csv.h"
#include "Table.h"
#include "sqlparser.h"
#include "cond.h"
#include "JoinAgent.h"

using namespace std;

vector<Table> tables;
map<string, int> tableId;
map<string, pair<int, int> > columnId;
vector<string> result;

set <int> filter(Table& t, const set <Cond>& FCond); 
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
	cout << sql << endl;
	if (strstr(sql.c_str(), "INSERT") != NULL) {
		tokenize(sql.c_str(), token);
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
	SQLParser sp(sql);

	initJoinGraph(sp);
	if (joinOrder != NULL)
		delete joinOrder;
	joinOrder = new int[JConds.size()*4];
	genJoinOrder(sp, joinOrder);

	JoinAgent ja(tables, JConds.size() + 1, joinOrder);
	set<int> f = filter(tables[joinOrder[0]], mttFCond[joinOrder[0]]);
	ja.init(f);
	for (int i = 0; i < JConds.size(); i ++) {
		f = filter(tables[joinOrder[i * 4 + 2]], mttFCond[joinOrder[i * 4 + 2]]);
		ja.join(i, f);
	}
	jaRet = ja.ret;	
	outputRowNum = 0;
	genOutput(0, BLOCK_SIZE);
}


int next(char *row)
{
	if (result.size() == 0){
		if (outputRowNum == jaRet.size()) {
			for (int i = 0; i < jaRet.size(); i++)
				delete jaRet[i];
			jaRet.clear();
			return (0);
		} else
			genOutput(outputRowNum + 1, BLOCK_SIZE);
	}
	strcpy(row, result.back().c_str());
	result.pop_back();
	outputRowNum++;
	printf("%s\n", row);

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

	for (int i = 0; i < tables.size(); i++) {
		mttRowNum[i] = tables[i].rows->count();
	}	
	for (int i = 0; i < sp.output.size(); i++) {
		int tid = columnId[sp.output[i]].first;
		int cid = columnId[sp.output[i]].second;
		mttcsProj[tid].insert(cid);
		outputCols.push_back(make_pair(tid, cid));	
	}
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
	for (int i = 0; i < sp.filter.size(); i++) {
		int tid = columnId[sp.filter[i].colName].first;
		int cid = columnId[sp.filter[i].colName].second;
		mttFCond[tid].insert(sp.filter[i]);	
		mttRowNum[tid] = mttRowNum[tid]/tables[tid].columns[cid].index->count();
	}
	for (int i = 0; i < sp.range.size(); i++) {
		int tid = columnId[sp.range[i].colName].first;
		mttFCond[tid].insert(sp.range[i]);	
		mttRowNum[tid] = mttRowNum[tid]/3;
	}
}

void genJoinOrder(SQLParser&sp, int* joinOrder) {
	int* q = joinOrder;
	int min = 2147483647;
	int curTable;
	for (int i = 0; i < sp.tables.size(); i++)
		if (mttRowNum[tableId[sp.tables[i]]] < min) {
			min = mttRowNum[tableId[sp.tables[i]]];
			curTable = tableId[sp.tables[i]];
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
				if (oldConds.find((*jt)) == oldConds.end())
					curConds.insert((*jt));
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

void genOutput(int start, int len) {
	byte* rowContent = NULL;
	vector <vector <string> > t_result;
	result.clear();
	for (int i = start; i < jaRet.size() && i < start + len; i++)
		t_result.push_back(vector <string>(outputCols.size()));
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
		for (int jj = start; jj < jaRet.size() && jj < start + len; jj++) {
			int j = jj - start;
			size_t rowLen;
			//find the col num in jaRet that store the rowId of table i
			int pos;
			for (pos = 0; pos < sizeof(joinOrder) / 4; pos++)
				if (joinOrder[pos * 4] == i)
					break;
			if (joinOrder[sizeof(joinOrder) * 4 - 2] == i)
				pos = sizeof(joinOrder);
			rowContent = tables[i].rows->get((byte*)&(jaRet[jj][pos]), 4, &rowLen);
			//get the data...
			for (int k = 0; k < cids.size(); k++) {
				int colOffset = tables[i].columns[cids[k]].offset;
				int colLen = tables[i].columns[cids[k]].len;
				if (tables[i].columns[cids[k]].type == INT) {
					unsigned int t = *(rowContent + colOffset);
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
		result.push_back(string());
		for (int k = 0; k < t_result.back().size() - 1; k++) {
			result[j] += t_result.back()[k] + ",";
		}
		result[j] += t_result.back()[t_result.back().size() - 1];
		t_result.pop_back();	
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

set <int> filter(Table& t, const set <Cond>& FCond) {
	bool found = false;
	set <int> s[2];
	vector <int> temp;
	int p = 0;
	for (set <Cond>::iterator it = FCond.begin(); it != FCond.end(); it++) {
		int tid = columnId[it->colName].first;
		int cid = columnId[it->colName].second;	
		temp.clear();
		switch(it->type) {
			case IFIL:
				tables[tid].columns[cid].filterBy(it->c_int, EQU, temp);
				break;
			case SFIL:
				tables[tid].columns[cid].filterBy(it->c_string, temp);
				break;
			case RANG:
				switch(it->op) {
					case '<':
						tables[tid].columns[cid].filterBy(it->c_int, LES, temp);
						break;
					case '>':
						tables[tid].columns[cid].filterBy(it->c_int, GTR, temp);
						break;
				}
				break;
		}	
		s[(p+1)&1].clear();
		for (int i = 0; i < temp.size(); i++) {
			if (!found || s[p].find(temp[i]) != s[p].end())
				s[(p+1)&1].insert(temp[i]);
		}
		found = true;
		p = (p+1)&1;
	}
	if (!found) {
		for (int i = 0; i < t.rows->count(); i++)
			s[p].insert(i);
	}
	return s[p];
}	
