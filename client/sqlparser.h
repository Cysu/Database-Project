#ifndef SQLP_H
#define SQLP_H
#include <vector>
#include <string>
#include "cond.h"

using namespace std;

class SQLParser {
	public:
		SQLParser(const string& sql);
		~SQLParser() {}
		vector <Cond> join;
		vector <Cond> filter;
		vector <Cond> range;
		vector <string> output;
		vector <string> token;
		vector <string> tables;
};
#endif
