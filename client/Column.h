#ifndef COLUMN_H
#define COLUMN_H

#include <string>
#include <cstring>
#include "kchashdb.h"
#include "const.h"
#include "utils.h"

using namespace std;
using namespace kyotocabinet;

class Column {
public:
	string name;
	COLUMN_TYPE type;
	int len;	// length of string(+1 for '\0')
	int offset;	// offset in a row(in bytes)
	TreeDB* index;
	bool needIndex, hasIndex;

	Column(const string& name, const string& type);
	void initIndex();
	void insertIndex(string key, int rowNum);
	void insertIndex(unsigned int key, int rowNum);

	void filterBy(unsigned int key, OPR_TYPE opr, vector<int>& ret);
	void filterBy(string key, vector<int>& ret);
	
};

#endif // COLUMN_H
