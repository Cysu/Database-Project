#ifndef UTILS_H
#define UTILS_H

#include "const.h"
#include <cstdio>
#include <string>

using namespace std;

extern FILE* logFile;
extern bool logSwitch;

void getBigNotation(unsigned int num, byte* buf);
void debugLog(string s);
void debugLog(int x);
void debugLog(unsigned int x);
void debugLog(double x);

#endif // UTILS_H
