#include "Column.h"

Column::Column(const string& name, const string& type) {
	this->name = name;
	if (type[0] == 'I') {
		len = 4;	// 1 int = 4 bytes
		this->type = INT;
	} else {
		sscanf(type.c_str(), "VARCHAR(%d)", &len);
		len += 3;		// for '\0' and '\'*2'
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
	//printf("%s, %d, %d\n", name.c_str(), key, rowNum);
}

void Column::insertIndex(string key, int rowNum) {
	const byte* kBuf = key.c_str();
	const byte* vBuf = (byte*) &rowNum;
	index->append(kBuf, key.length(), vBuf, 4);
	//printf("%s, %s, %d\n", name.c_str(), key.c_str(), rowNum);
}

vector<int> Column::filterBy(int key, OPR_TYPE opr) {
	vector<int> ret;
	DB::Cursor* cur = index->cursor();
	size_t size;
	byte* vBuf;
	const byte* kBuf;
	bool exist;
	switch (opr) {
		case EQU:
			kBuf = (byte*) &key;
			exist = cur->jump(kBuf, 4);
			if (exist) {
				vBuf = cur->get_value(&size, false);
				for (int i = 0; i < size; i += 4) {
					int t = *((int*)(vBuf + i));
					ret.push_back(t);
				}
			}
			break;
		case LES:
			// assert key != INT_MIN
			key --;
			kBuf = (byte*) &key;
			exist = cur->jump_back(kBuf, 4);
			while (exist) {
				vBuf = cur->get_value(&size, false);
				for (int i = 0; i < size; i += 4) {
					int t = *((int*)(vBuf + i));
					ret.push_back(t);
				}
				exist = cur->step_back();
			}
			break;
		case GTR:
			// assert key != INT_MAX
			key ++;
			kBuf = (byte*) &key;
			exist = cur->jump(kBuf, 4);
			while (exist) {
				vBuf = cur->get_value(&size, false);
				for (int i = 0; i < size; i += 4) {
					int t = *((int*)(vBuf + i));
					ret.push_back(t);
				}
				exist = cur->step();
			}
			break;
	}
	delete cur;
	return ret;
}

