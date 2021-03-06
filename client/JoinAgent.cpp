#include "JoinAgent.h"

JoinAgent::JoinAgent(
		vector<Table>& tables,
		int n,
		int* order) :
		tables(tables)
{
	this->n = n;
	this->order = order;
}

void JoinAgent::init(const set<int>& filterRet) {
	if (filterRet.size() == 0) {
		int num = tables[order[0]].rows->count();
		for (int i = 0; i < num; i++) {
			int* joinRetItem = new int[n];
			joinRetItem[0] = i;
			ret.push_back(joinRetItem);
		}
	} else if (filterRet.find(-1) == filterRet.end()) {
		for (set<int>::iterator iter = filterRet.begin(); iter != filterRet.end(); iter ++) {
			int* joinRetItem = new int[n];
			joinRetItem[0] = *iter;
			ret.push_back(joinRetItem);
		}
	}
}

void JoinAgent::join(int i, const set<int>& filterRet) {
	int tIdA = order[i * 4];
	int cIdA = order[i * 4 + 1];
	int tIdB = order[i * 4 + 2];
	int cIdB = order[i * 4 + 3];

	// sort
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
	
void JoinAgent::addTo(vector<int*>& newRet, int j, int i, const vector<int>& matchRows, const set<int>& filterRet) {
	for (int k = 0; k < matchRows.size(); k ++) {
		if (filterRet.size() == 0 || filterRet.find(matchRows[k]) != filterRet.end()) {
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
