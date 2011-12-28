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
	int len;
	TreeDB* index;

	Column(const string& name, const string& type);
};

#endif // COLUMN_H
