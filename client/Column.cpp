#include "Column.h"

Column::Column(const string& name, const string& type) {
	this->name = name;
	if (type[0] == 'I') {
		len = 4;
		this->type = INT;
	} else {
		sscanf(type.c_str(), "VARCHAR(%d)", &len);
		len += 3;
		this->type = STRING;
	}
	needIndex = false;
	diffKey = 0;
	minKey = 2147483647;
	maxKey = 0;
}

void Column::insertIndex(unsigned int key, int rowNum) {
	if (intIndex.find(key) == intIndex.end())
		diffKey ++;
	minKey = minKey > key ? key : minKey;
	maxKey = maxKey < key ? key : maxKey;
	intIndex.insert(make_pair(key, rowNum));
}

void Column::insertIndex(string key, int rowNum) {
	if (strIndex.find(key) == strIndex.end())
		diffKey ++;
	strIndex.insert(make_pair(key, rowNum));
}

void Column::filterBy(unsigned int key, OPR_TYPE opr, vector<int>& ret) {
	pair<multimap<unsigned int, int>::iterator, multimap<unsigned int, int>::iterator> range;
	multimap<unsigned int, int>::iterator low, up;
	switch (opr) {
		case EQU:
			range = intIndex.equal_range(key);
			for (multimap<unsigned int, int>::iterator it = range.first;
					it != range.second; it ++) {
				ret.push_back(it->second);
			}
			break;
		case LES:
			low = intIndex.lower_bound(0);
			up = intIndex.upper_bound(key - 1);
			for (multimap<unsigned int, int>::iterator it = low; it != up; it ++)
				ret.push_back(it->second);
			break;
		case GTR:
			low = intIndex.lower_bound(key + 1);
			up = intIndex.upper_bound(2147483647);
			for (multimap<unsigned int, int>::iterator it = low; it != up; it ++)
				ret.push_back(it->second);
			break;
		default:
			break;
	}
}

void Column::filterBy(string key, vector<int>& ret) {
	pair<multimap<string, int>::iterator, multimap<string, int>::iterator> range;
	range = strIndex.equal_range(key);
	for (multimap<string, int>::iterator it = range.first;
			it != range.second; it ++) {
		ret.push_back(it->second);
	}
}

int Column::getIndexCount(OPR_TYPE type, unsigned int key) {
	if (type == EQU)
		return intIndex.size() / diffKey;
	else if (type == LES)
		return (key - minKey) * 1.0 / (maxKey - minKey) * intIndex.size();
	else if (type == GTR)
		return (maxKey - key) * 1.0 / (maxKey - minKey) * intIndex.size();
}

int Column::getIndexCount(string key) {
	return strIndex.size() / diffKey;
}

