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

const int COLUMN_MAX_LENGTH = 520;
const int MAX_INT = 2147483647;
const int MAX_TABLES = 1000;
const int BLOCK_SIZE = 16384; //the size of the return vector 

#endif // CONST_H
