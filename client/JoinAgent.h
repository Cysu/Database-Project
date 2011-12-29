#ifndef JOINAGENT_H
#define JOINAGENT_H

#include "Table.h"
#include "utils.h"
#include <vector>
#include <map>
#include <set>
#include <utility>

using namespace std;


class JoinAgent {
public:
	vector<int*> ret;

	vector<Table>& tables;
	int n;
	int* order;

	JoinAgent(
		vector<Table>& tables,
		int n,
		int* order
	);

	void init(const set<int>& filterRet);
	void join(int i, const set<int>& filterRet);

private:
	void sort(int t, int l, int r);
	void addTo(int j, int i, const vector<int>& matchRows, const set<int>& filterRet);
};

#endif // JOINAGENT_H
