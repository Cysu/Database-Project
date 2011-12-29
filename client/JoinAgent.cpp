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
	for (set<int>::iterator iter = filterRet.begin(); iter != filterRet.end(); iter ++) {
		int* joinRetItem = new int[n];
		joinRetItem[0] = *iter;
		ret.push_back(joinRetItem);
	}
}

void JoinAgent::join(int i, const set<int>& filterRet) {
	int tIdA = order[i * 4];
	int cIdA = order[i * 4 + 1];
	int tIdB = order[i * 4 + 2];
	int cIdB = order[i * 4 + 3];

	// sort
	sort(i, 0, ret.size() - 1);

	vector<int*> newRet;

	byte* rowContent = NULL;
	vector<int> matchRows;
	for (int j = 0; j < ret.size(); j ++) {
		if (j == 0 || ret[j][i] != ret[j-1][i]) {
			// find in index
			size_t rowLen;
			int tmp = 0;
			matchRows.clear();
			rowContent = tables[tIdA].rows->get((byte*) &(ret[j][i]), 4, &rowLen);
			int colOffset = tables[tIdA].columns[cIdA].offset;
			int colLen = tables[tIdA].columns[cIdA].len;
			if (tables[tIdA].columns[cIdA].type == INT) {
				unsigned int t = *(rowContent + colOffset);
				tables[tIdB].columns[cIdB].filterBy(t, EQU, matchRows);
			} else {
				string t = rowContent + colOffset;
				tables[tIdB].columns[cIdB].filterBy(t, matchRows);
			}
		}
		addTo(newRet, j, i, matchRows, filterRet);
		output(newRet);
	}

	for (int j = 0; j < ret.size(); j ++)
		delete ret[j];
	ret = newRet;
}

void JoinAgent::sort(int t, int l, int r) {
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
	if (i < r) sort(t, i, r);
	if (l < j) sort(t, l, j);
}
	
void JoinAgent::addTo(vector<int*>& newRet, int j, int i, const vector<int>& matchRows, const set<int>& filterRet) {
	for (int k = 0; k < matchRows.size(); k ++) {
		if (filterRet.find(matchRows[k]) != filterRet.end()) {
			int* joinRetItem = new int[n];
			memcpy(joinRetItem, ret[j], sizeof(joinRetItem));
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
