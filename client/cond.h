#ifndef COND_H
#define COND_H

#include <string>

using namespace std;

const char JOIN = 1;
const char IFIL = 2;
const char SFIL = 3;
const char RANG = 4;

class Cond {
	public:
	char type; 
	string colA, colB;
	char op;//indicate <>
	string c_string;
	int c_int;
	string colName;
	bool operator <(const Cond& b) const {
		if (type != b.type)
			return type < b.type;
		else {
			switch(type) {
				case JOIN:
					if (colA < b.colA)
						return true;
					else if (colA == b.colA)
						return colB < b.colB;
				case IFIL:
					if (colName == b.colName)
						return c_int < b.c_int;
					else
						return colName < b.colName;
				case SFIL:
					if (colName == b.colName)
						return c_string < b.c_string;
					else
						return colName < b.colName;
				case RANG:
					if (colName == b.colName)
						return op < b.op;
					else
						return colName < b.colName;

			}
		
		}
	}
};
#endif
