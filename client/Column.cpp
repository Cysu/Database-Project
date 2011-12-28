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
}

void Column::createIndex(HashDB* rows) {
	index = new TreeDB();
	index->open(("data/" + name + ".kch").c_str(), TreeDB::OWRITER | TreeDB:: OCREATE);
	DB::Cursor* cur = rows->cursor();
	cur->jump();
	size_t len1, len2;
	byte* rowNum = new byte[4];
	const byte* rowContent;
	while ((rowNum = cur->get(&len1, &rowContent, &len2, true)) != NULL) {
		printf("%d,%s,%d\n", *rowNum, rowContent + offset, strlen(rowContent+offset));
		index->set((byte*)(rowContent + offset), strlen(rowContent+offset), rowNum, 4);
	}
}
