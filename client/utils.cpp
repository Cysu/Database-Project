#include "utils.h"

void getBigNotation(unsigned int num, byte* buf) {
	byte* tmp = (byte*) &num;
	for (int i = 0; i < 4; i ++)
		buf[i] = tmp[3 - i];
}
