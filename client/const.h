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
const int ROW_MAX_LENGTH = 10000;
const int BLOCK_SIZE = 65536; //the size of the return vector 

#endif // CONST_H
