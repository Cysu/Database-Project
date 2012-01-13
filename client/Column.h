#ifndef COLUMN_H
#define COLUMN_H

#include <cstring>
#include <map>
#include <vector>
#include "utils.h"

class Column {
public:
	string name;
	COLUMN_TYPE type;
	int len, offset;

	bool needIndex;
	multimap<unsigned int, int> intIndex;
	multimap<string, int> strIndex;
	int diffKey;
	unsigned minKey, maxKey;

	Column(const string& name, const string& type);

	void insertIndex(string key, int rowNum);
	void insertIndex(unsigned int key, int rowNum);

	void filterBy(unsigned int key, OPR_TYPE opr, vector<int>& ret);
	void filterBy(string key, vector<int>& ret);

	int getIndexCount(OPR_TYPE type, unsigned int key);
	int getIndexCount(string key);
};

#endif // COLUMN_H


