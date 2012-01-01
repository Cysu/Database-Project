#include "utils.h"

void getBigNotation(unsigned int num, byte* buf) {
	byte* tmp = (byte*) &num;
	for (int i = 0; i < 4; i ++)
		buf[i] = tmp[3 - i];
}

unsigned int getSmallNotation(byte* buf) {
	int ret = 0;
	for (int i = 0; i < 4; i++) {
		ret <<= 4;
		ret += buf[i];
	}
	return ret;
}

