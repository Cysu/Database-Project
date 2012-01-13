#include "utils.h"

FILE* logFile = NULL;
bool logSwitch = false;

void getBigNotation(unsigned int num, byte* buf) {
	byte* tmp = (byte*) &num;
	for (int i = 0; i < 4; i ++)
		buf[i] = tmp[3 - i];
}

void debugLog(string s) {
	if (logSwitch) {
		printf("%s\n", s.c_str());
	}
}

void debugLog(int x) {
	if (logSwitch) {
		printf("%d\n", x);
	}
}

void debugLog(unsigned int x) {
	if (logSwitch) {
		printf("%u\n", x);
	}
}

void debugLog(double x) {
	if (logSwitch) {
		printf("%lf\n", x);
	}
}

