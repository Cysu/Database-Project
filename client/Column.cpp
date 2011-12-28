#include "Column.h"

Column::Column(const string& name, const string& type) {
	this->name = name;
	if (type[0] == 'I') {
		len = 4;	// 1 int = 4 bytes
		this->type = INT;
	} else {
		sscanf(type.c_str(), "VARCHAR(%d)", &len);
		len ++;		// for '\0'
		this->type = STRING;
	}
	index = NULL;
	needIndex = false;
}

void Column::initIndex() {
	index = new TreeDB();
	index->open(("data/" + name + ".kch").c_str(), TreeDB::OWRITER | TreeDB:: OCREATE);
}

void Column::insertIndex(int key, int rowNum) {
	const byte* kBuf = (byte*) &key;
	const byte* vBuf = (byte*) &rowNum;
	index->append(kBuf, 4, vBuf, 4);
	printf("%s, %d, %d\n", name.c_str(), key, rowNum);
}

void Column::insertIndex(string key, int rowNum) {
	const byte* kBuf = key.c_str();
	const byte* vBuf = (byte*) &rowNum;
	index->append(kBuf, key.length(), vBuf, 4);
	printf("%s, %s, %d\n", name.c_str(), key.c_str(), rowNum);
}

