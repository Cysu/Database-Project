#ifndef CONST_H
#define CONST_H

enum COLUMN_TYPE {
	INT,
	STRING
};

enum OPR_TYPE {
	EQU,
	LES,
	GTR
};

typedef char byte;

const int COLUMN_MAX_LENGTH = 256;
const int ROW_MAX_LENGTH = 10000;

#endif // CONST_H
