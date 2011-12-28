#ifndef COLUMN_H
#define COLUMN_H

#include <string>
#include <cstring>
#include <kchashdb.h>
#include "const.h"

using namespace std;
using namespace kyotocabinet;

class Column {
public:
	string name;
	COLUMN_TYPE type;
	int len;	// length of string(+1 for '\0')
	int offset;	// offset in a row(in bytes)
	TreeDB* index;
	bool needIndex;

	Column(const string& name, const string& type);
	void initIndex();
	void insertIndex(string key, int rowNum);
	void insertIndex(int key, int rowNum);
};

#endif // COLUMN_H
