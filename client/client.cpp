#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <map>
#include <cassert>
#include <iostream>
#include <utility>

#include "../include/client.h"
#include "../lib/tokenize.h"
#include "../lib/split_csv.h"
#include "Table.h"
#include "sqlparser.h"
#include "cond.h"

using namespace std;

map<string, vector<string> > table2name;
map<string, vector<string> > table2type;
map<string, vector<string> > table2pkey;
vector<string> result;

vector<Table> tables;
map<string, int> tableId;
map<string, pair<int, int> > columnId;

void done(const vector<string>& table, const map<string, int>& m,
	int depth, vector<string>& row)
{
	FILE *fin;
	char buf[65536];
	vector<string> column_name, token;
	string str;
	int i;

	if (depth == table.size()) {
		str = row[0];
		for (i = 1; i < row.size(); i++)
			str += "," + row[i];
		result.push_back(str);
		return;
	}

	assert(table2name.find(table[depth]) != table2name.end());
	column_name = table2name[table[depth]];

	fin = fopen(((string) "data/" + table[depth]).c_str(), "r");
	assert(fin != NULL);

	while (fgets(buf, 65536, fin) != NULL) {
		int len = strlen(buf);
		if (len > 0 && buf[len - 1] == '\n') {
			buf[len - 1] = '\0';
			len--;
		}
		if (len == 0)
			continue;

		split_csv(buf, token);
		assert(token.size() == column_name.size());

		for (i = 0; i < column_name.size(); i++)
			if (m.find(column_name[i]) != m.end())
				row[m.find(column_name[i]) -> second] = token[i];

		done(table, m, depth + 1, row);
	}

	fclose(fin);
}

void create(const string& table_name, const vector<string>& column_name,
	const vector<string>& column_type, const vector<string>& primary_key)
{
	Table table(table_name, column_name, column_type, primary_key);
	tables.push_back(table);
	tableId[table_name] = tables.size() - 1;
	for (int i = 0; i < column_name.size(); i ++)
		columnId[column_name[i]] = make_pair(tables.size() - 1, i);
}

void train(const vector<string>& query, const vector<double>& weight)
{
	/* I am too clever; I don't need it. */
	pair<int, int> tmp;
	for (int i = 0; i < query.size(); i++) {
		SQLParser sp(query[i]);
		cout << "Join:";
		for (int j = 0; j < sp.join.size(); j++) {
			cout << sp.join[j].colA << "=" << sp.join[j].colB;
			cout << " ";

			// mark to index
			tmp = columnId[sp.join[j].colA];
			tables[tmp.first].columns[tmp.second].needIndex = true;
			tmp = columnId[sp.join[j].colB];
			tables[tmp.first].columns[tmp.second].needIndex = true;
		}
		cout << "\nFilter:";
		for (int j = 0; j < sp.filter.size(); j++) {
			cout << sp.filter[j].colName << "=";
			if (sp.filter[j].type == IFIL)
				cout << sp.filter[j].c_int;
			else
				cout << sp.filter[j].c_string;
			cout << " ";

			tmp = columnId[sp.filter[j].colName];
			tables[tmp.first].columns[tmp.second].needIndex = true;
		}
		cout << "\nRange:";
		for (int j = 0; j < sp.range.size(); j++) {
			cout << sp.range[j].colName << sp.range[j].op << sp.range[j].c_int;
			cout << " ";

			tmp = columnId[sp.range[j].colName];
			tables[tmp.first].columns[tmp.second].needIndex = true;
		}	
		cout << "\n";
	}
}

void load(const string& table_name, const vector<string>& row)
{
	tables[tableId[table_name]].load(row);
}

void preprocess()
{
	vector<int> ret = tables[0].columns[0].filterBy(5, GTR);
	for (int i = 0; i < ret.size(); i ++)
		cout << ret[i] << " ";
	cout << endl;
}

void execute(const string& sql)
{
	vector<string> token, output, table, row;
	map<string, int> m;
	int i;

	result.clear();
	cout << sql << endl;
	if (strstr(sql.c_str(), "INSERT") != NULL) {
		tokenize(sql.c_str(), token);
		i += 2;//skip INSERT INTO
		Table& t = tables[tableId[token[i++]]];	
		for (i++; i < token.size(); i++) {
			if (token[i] == ";" || token[i] == ",")
				continue;
			t.insert(token[i].substr(1, token[i].size() - 2));	
		}
	}

}

int next(char *row)
{
	if (result.size() == 0)
		return (0);
	strcpy(row, result.back().c_str());
	result.pop_back();

	/*
	 * This is for debug only. You should avoid unnecessary output
	 * when submitting, which will hurt the performance.
	 */
	printf("%s\n", row);

	return (1);
}

void close()
{
	/* I have nothing to do. */
}


