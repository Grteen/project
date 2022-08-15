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
    int INT_CHECK();
    INT();
    ~INT();
};

class DOUBLE {
public:
    double val;
    RANGE DOUBLE_RANGE;
    int DOUBLE_CHECK();
    DOUBLE();
    ~DOUBLE();
};

class FLOAT {
public:
    float val;
    RANGE FLOAT_RANGE;
    int FLOAT_CHECK();
    FLOAT();
    ~FLOAT();
};

// string type
class CHAR {
public:
    std::string val;
    RANGE CHAR_RANGE;
    int CHAR_CHECK();
    CHAR();
    ~CHAR();
    CHAR(unsigned int size);
};

class VARCHAR {
public:
    std::string val;
    RANGE VARCHAR_RANGE;
    int VARCHAR_CHECK();
    VARCHAR();
    ~VARCHAR();
    VARCHAR(unsigned int size);
};

// time type
class YEAR {
public:
    int val;
    RANGE YEAR_RANGE;
    int YEAR_CHECK();
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
    int TIME_CHECK();
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
    int DATE_CHECK();      // check DATE's  validity
};
