#ifndef COND_H
#define COND_H

#include <string>

using namespace std;

const char JOIN = 1;
const char FILT = 2;
const char RANG = 3;

struct Cond {
	char type; 
	string colA, colB;
	char op;//indicate <>
	string c_string;
	int c_int;
	string colName;
};
#endif
