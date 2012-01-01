#ifndef JOINAGENT_H
#define JOINAGENT_H

#include "Table.h"
#include "utils.h"
#include <vector>
#include <map>
#include <set>
#include <ext/hash_set>
#include <utility>

using namespace std;
using namespace __gnu_cxx;

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

	void init(const hash_set<int>& filterRet);
	void join(int i, const hash_set<int>& filterRet);

	void output(const vector<int*>& ret);

	static void sort(vector<int*>& ret, int t, int l, int r);
private:
	void addTo(vector<int*>& newRet, int j, int i, const vector<int>& matchRows, const hash_set<int>& filterRet);
	void indexJoin(int i, const hash_set<int>& filterRet);
	void checkJoin(int i, const hash_set<int>& filterRet);
};


#endif // JOINAGENT_H
