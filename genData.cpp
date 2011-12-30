#include <cstdio>
#include <cstring>
#include <string>
#include <cstdlib>
#include <cmath>
#include <ctime>
#include <set>

using namespace std;

const int MAX_INT = 30;
const int MAX_STRING = 6;

int getRandomInt(int range) {
	return rand() % range;
}

string getRandomString(int range) {
	int len = rand() % range + 1;
	char buf[len + 3];
	buf[0] = '\'';
	for (int p = 1; p <= len; p ++)
		buf[p] = 'a' + rand() % 26;
	buf[len + 1] = '\'';
	buf[len + 2] = '\0';
	return buf;
}

string itos(int x) {
	char buf[16];
	sprintf(buf, "%d", x);
	return buf;
}

int nTables, nRows, nCols, nQuery;

int main(int argc, char* argv[]) {
	srand(time(0));

	nTables = atoi(argv[1]);
	nRows = atoi(argv[2]);
	nCols = atoi(argv[3]);
	nQuery = atoi(argv[4]);

	// schema
	FILE* fSchema = fopen("test/schema", "w");
	fprintf(fSchema, "%d\n", nTables);
	for (int i = 0; i < nTables; i ++) {
		fprintf(fSchema, "%c\n", 'A' + i);
		fprintf(fSchema, "%d\n", nCols);
		for (int j = 0; j < nCols; j ++) {
			char buf[16];
			sprintf(buf, "%c_%d", 'a' + i, j);
			if (j % 2 == 0)
				fprintf(fSchema, "%s\t\tINTEGER\n", buf);
			else
				fprintf(fSchema, "%s\t\tVARCHAR(256)\n", buf);
		}
		fprintf(fSchema, "%d\n", 0);
	}
	fclose(fSchema);

	// statics and query
	FILE* fQuery = fopen("test/query", "w");
	FILE* fStatistic = fopen("test/statistic", "w");
	fprintf(fQuery, "%d\n", nQuery);
	fprintf(fStatistic, "%d\n", nQuery);
	for (int i = 0; i < nQuery; i ++) {
		// gen tables' id to select
		int m = min(rand() % 4 + 1, nTables);
		int qTables[m];
		set<int> selected;
		for (int j = 0; j < m; j ++) {
			int t;
			while (true) {
				t = rand() % nTables;
				if (selected.find(t) == selected.end()) {
					selected.insert(t);
					break;
				}
			}
			qTables[j] = t;
		}

		// SELECT a_1, b_0
		string query = "SELECT ";
		for (int j = 0; j < m; j ++) {
			int t = qTables[j];
			query += ('a' + t);
			query += "_1";
			if (j != m - 1) query += ',';
			query += ' ';
		}

		// FROM A, B
		query += "FROM ";
		for (int j = 0; j < m; j ++) {
			int t = qTables[j];
			query += ('A' + t);
			if (j != m - 1) query += ',';
			query += ' ';
		}

		// WHERE a_2 = b_2 AND a_0 = c_0
		query += "WHERE ";
		for (int j = 1; j < m; j ++) {
			int ta = qTables[rand() % j];
			int tb = qTables[j];
			int ca = rand() % nCols;
			int cb = ca;
			query += ('a' + ta);
			query += '_';
			query += itos(ca);
			query += " = ";
			query += ('a' + tb);
			query += '_';
			query += itos(cb);
			if (j != m - 1) query += " AND ";
		}

		// AND a_0 = 1 AND a_1 = 'haha' AND d_8 = 1
		int nConds = rand() % 10;
		for (int j = 0; j < nConds; j ++) {
			if (j > 0 || m > 1) query += " AND ";
			int t = qTables[rand() % m];
			int c = rand() % nCols;
			query += ('a' + t);
			query += '_';
			query += itos(c);
			query += " = ";

			if (c % 2 == 0) {
				query += itos(getRandomInt(MAX_INT));
			} else {
				string buf = getRandomString(MAX_STRING);
				query += buf;
			}
		}
		fprintf(fQuery, "%s\n", query.c_str());
		fprintf(fStatistic, "%s\n%0.1lf\n", query.c_str(), 100.0 / nQuery);
	}
	fclose(fQuery);
	fclose(fStatistic);
	
	// *.data
	for (int i = 0; i < nTables; i ++) {
		char fileBuf[16];
		sprintf(fileBuf, "test/%c.data", 'A' + i);
		FILE* fData = fopen(fileBuf, "w");
		fprintf(fData, "%d\n", nRows);
		for (int j = 0; j < nRows; j ++) {
			fprintf(fData, "%d", getRandomInt(MAX_INT));
			for (int k = 1; k < nCols; k ++) {
				if (k % 2 == 0) {
					fprintf(fData, ",%d", getRandomInt(MAX_INT));
				} else {
					string buf = getRandomString(MAX_STRING);
					fprintf(fData, ",%s", buf.c_str());
				}
			}
			fprintf(fData, "\n");
		}
		fclose(fData);
	}

	return 0;
}

