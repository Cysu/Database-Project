#include "Column.h"

Column::Column(const string& name, const string& type) {
	this.name = name;
	if (type[0] == 'I') {
		len = 1;
		this.type = INT;
	} else {
		sprintf(type.c_str(), "VARCHAR(%d)", len);
		this.type = STRING;
	}
	index = NULL;
}
