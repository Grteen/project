#include "common.h"
#include <string>

class RANGE {
public:
    long long max;
    long long min;
};

// number type
class INT {
public:
    int val;
    RANGE INT_RANGE;
    INT();
    ~INT();
};

class DOUBLE {
public:
    double val;
    RANGE DOUBLE_RANGE;
    DOUBLE();
    ~DOUBLE();
};

class FLOAT {
public:
    float val;
    RANGE FLOAT_RANGE;
    FLOAT();
    ~FLOAT();
};

// string type
class CHAR {
public:
    std::string val;
    RANGE CHAR_RANGE;
    CHAR();
    ~CHAR();
    CHAR(unsigned int size);
};

class VARCHAR {
public:
    std::string val;
    RANGE VARCHAR_RANGE;
    VARCHAR();
    ~VARCHAR();
    VARCHAR(unsigned int size);
};

// time type
class YEAR {
public:
    int val;
    RANGE YEAR_RANGE;
    YEAR();
    ~YEAR();
};

class TIME {
public:
    int hour;
    RANGE TIME_HOUR_RANGE;
    int minute;
    RANGE TIME_MINUTE_RANGE;
    int second;
    RANGE TIME_SECOND_RANGE;
    TIME();
    ~TIME();
};

class DATE {        // val checked by other function
public:
    int year;
    RANGE DATE_YEAR_RANGE;       
    int month;
    int day;
    DATE();
    ~DATE();
};

int DATE_checkfunction(DATE date);      // check DATE's  validity
