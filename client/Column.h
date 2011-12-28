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

	Column(const string& name, const string& type);
	void createIndex(HashDB* rows);
};

#endif // COLUMN_H
