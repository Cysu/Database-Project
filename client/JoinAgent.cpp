#include "JoinAgent.h"
#include "utils.h"

JoinAgent::JoinAgent(
		vector<Table>& tables,
		int n,
		int* order) :
		tables(tables)
{
	this->n = n;
	this->order = order;
}

void JoinAgent::init(const hash_set<int>& filterRet) {
	for (hash_set<int>::iterator iter = filterRet.begin(); iter != filterRet.end(); iter ++) {
		int* joinRetItem = new int[n];
		joinRetItem[0] = *iter;
		ret.push_back(joinRetItem);
	}
}

void JoinAgent::join(int i, const hash_set<int>& filterRet) {
	int tIdA = order[i * 4];
	int cIdA = order[i * 4 + 1];
	int tIdB = order[i * 4 + 2];
	int cIdB = order[i * 4 + 3];
	hash_set <int> distinct;
	for (int j = 0; j < ret.size(); j++)
		distinct.insert(ret[j][i]);
	//int count = 1;
	//for (int j = 0; j < ret.size()-1; j++)
	//	if (ret[j][i] != ret[j + 1][i])
	//		count++;
	if (distinct.size()  < (tables[tIdA].columns[cIdA].index->count() + tables[tIdB].columns[cIdB].index->count()) / 3) {
		//assume that access data table is 20 times slower than access index table
		cout << "check" << endl;
		checkJoin(i, filterRet);
	} else {
		cout << "index" << endl;
		indexJoin(i, filterRet);
	}
	cout << "done" << endl;
}

void JoinAgent::indexJoin(int i, const hash_set<int>& filterRet) {
	int tIdA = order[i * 4];
	int cIdA = order[i * 4 + 1];
	int tIdB = order[i * 4 + 2];
	int cIdB = order[i * 4 + 3];
	size_t size;	
	//map from rIdA to rIdBs
	map <int, vector <int> > a2bs;
	if (tables[tIdA].columns[cIdA].type == INT) {
       		byte* kBuf;
		byte* vBufA;
	       	byte* vBufB;
		DB::Cursor* ca = tables[tIdA].columns[cIdA].index->cursor();
		DB::Cursor* cb = tables[tIdB].columns[cIdB].index->cursor();
		bool exist1 = ca->jump();
		bool exist2 = cb->jump();
		while (exist1 && exist2) {
			size = 4;
			unsigned int keyA, keyB;
			kBuf = ca->get_key(&size, false);
			keyA = getSmallNotation(kBuf);
			kBuf = cb->get_key(&size, false);
			keyB = getSmallNotation(kBuf);
			//cout << "a:" << keyA;
			//cout << "b:" << keyB << endl;
			if (keyA < keyB) {
				//cout << "a++" << endl;
				exist1 = ca->step();
			} else if (keyA > keyB) {
				//cout << "b++" << endl;
				exist2 = cb->step();
			} else {
				vBufB = cb->get_value(&size, false);
				vector <int> rIdBs;
				for (int i = 0; i < size; i+= 4) {	
					int t = *((int*)(vBufB + i));
					rIdBs.push_back(t);
				}
				delete vBufB;
				vBufA = ca->get_value(&size, false);
				for (int i = 0; i < size; i+= 4) {
					int t = *((int*)(vBufA + i));
					a2bs[t] = rIdBs;
				}
				exist1 = ca->step();
				exist2 = cb->step();
				delete vBufA;
			}
			delete kBuf;
		}
	} else {
		string keyA, keyB;
		byte* vBufA;
		byte* vBufB;
		DB::Cursor* ca = tables[tIdA].columns[cIdA].index->cursor();
		DB::Cursor* cb = tables[tIdB].columns[cIdB].index->cursor();
		bool exist1 = ca->jump();
		bool exist2 = cb->jump();
		while (exist1 && exist2) {
			ca->get_key(&keyA, false);
			cb->get_key(&keyB, false);
			if (keyA < keyB)
				exist1 = ca->step();
			else if (keyA > keyB)
				exist2 = cb->step();
			else {
				vBufB = cb->get_value(&size, false);
				vector <int> rIdBs;
				for (int i = 0; i < size; i+= 4) {	
					int t = *((int*)(vBufB + i));
					rIdBs.push_back(t);
				}
				vBufA = ca->get_value(&size, false);
				for (int i = 0; i < size; i+= 4) {
					int t = *((int*)(vBufA + i));
					a2bs[t] = rIdBs;
				}
				exist1 = ca->step();
				exist2 = cb->step();
				delete vBufA;
				delete vBufB;
			}
		}
	}
	vector <int*> newRet;
	for (int j = 0; j < ret.size(); j++) {
		addTo(newRet, j, i, a2bs[ret[j][i]], filterRet);
	}
	for (int j = 0; j < ret.size(); j++)
		delete ret[j];
	ret = newRet;
		
}

void JoinAgent::checkJoin(int i, const hash_set<int>& filterRet) {
	int tIdA = order[i * 4];
	int cIdA = order[i * 4 + 1];
	int tIdB = order[i * 4 + 2];
	int cIdB = order[i * 4 + 3];

	// sort move to join
	sort(ret, i, 0, ret.size() - 1);
	vector<int*> newRet;

	byte* rowContent = NULL;
	vector<int> matchRows;
	//cout << ret.size() << endl;
	for (int j = 0; j < ret.size(); j ++) {
		if (j == 0 || ret[j][i] != ret[j-1][i]) {
			// find in index
			size_t rowLen;
			int tmp = 0;
			matchRows.clear();
			byte kBuf[4];
			getBigNotation(ret[j][i], kBuf);
			rowContent = tables[tIdA].rows->get(kBuf, 4, &rowLen);
			int colOffset = tables[tIdA].columns[cIdA].offset;
			int colLen = tables[tIdA].columns[cIdA].len;
			// debug
			//printf("table %d, row %d, col %d, value ", tIdA, ret[j][i], cIdA);
			if (tables[tIdA].columns[cIdA].type == INT) {
				unsigned int t = *((unsigned int*)(rowContent + colOffset));
				tables[tIdB].columns[cIdB].filterBy(t, EQU, matchRows);
			//	cout << t << endl;
			} else {
				string t = rowContent + colOffset;
				tables[tIdB].columns[cIdB].filterBy(t, matchRows);
			//	cout << t << endl;
			}
			delete rowContent;

		}
		//cout << "matches " << matchRows.size() << endl;
		addTo(newRet, j, i, matchRows, filterRet);
	}


	for (int j = 0; j < ret.size(); j ++)
		delete ret[j];
	ret = newRet;
}

void JoinAgent::sort(vector<int*>& ret, int t, int l, int r) {
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
	
void JoinAgent::addTo(vector<int*>& newRet, int j, int i, const vector<int>& matchRows, const hash_set<int>& filterRet) {
	for (int k = 0; k < matchRows.size(); k ++) {
		if (filterRet.find(matchRows[k]) != filterRet.end()) {
			int* joinRetItem = new int[n];
			memcpy(joinRetItem, ret[j], n * 4);
			joinRetItem[i + 1] = matchRows[k];
			newRet.push_back(joinRetItem);
		}
	}
}

void JoinAgent::output(const vector<int*>& ret) {
	printf("output:\n");
	for (int i = 0; i < ret.size(); i ++) {
		for (int j = 0; j < n; j ++) printf("%d ", ret[i][j]);
		printf("\n");
	}
}
