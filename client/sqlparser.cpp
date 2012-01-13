#include "sqlparser.h"
#include "lib_tokenize.h"
#include <cstdio>

SQLParser::SQLParser(const string& sql) {
	
	lib_tokenize(sql.c_str(), token);
	int i;
	output.clear();
	for(i = 0; i < token.size(); i++) {
		if (token[i] == "SELECT" || token[i] == ",")
			continue;
		if (token[i] == "FROM")
			break;
		output.push_back(token[i]);
	}
	tables.clear();
	for(i++; i < token.size(); i++) {
		if (token[i] == ",")
			continue;
		if (token[i] == "WHERE" || token[i] == ";")
			break;
		tables.push_back(token[i]);
	}

	join.clear();
	filter.clear();
	range.clear();
	
	string colx;
	for(i++; i < token.size(); i++) {
		if (token[i] == ";" || token[i] == "AND")
			continue;
		colx = token[i++];
		if (token[i] == "=") {
			i++;
			Cond c;
			if (token[i][0] == '\'' ||
			    (token[i][0] >= '0' && 
			    token[i][0] <= '9')) {
				c.colName = colx;
				if (token[i][0] == '\'') {
					c.type = SFIL;
					c.c_string = token[i];
				} else {
					c.type = IFIL;
					sscanf(token[i].c_str(), "%d", &(c.c_int));
				}
				filter.push_back(c);
			} else {
				c.type = JOIN;
				c.colA = colx;
				c.colB = token[i];
				join.push_back(c);
			}
		} else if (token[i] == "<" || token[i] == ">") {
			Cond c;
			c.type = RANG;
			c.op = token[i][0];
			c.colName = colx;
			i++;
			sscanf(token[i].c_str(), "%d", &(c.c_int));
			range.push_back(c);
		} 
	}
}
