#include "db_type.h"

INT::INT() {
    this->INT_RANGE.max = INT_MAX;
    this->INT_RANGE.min = INT_MIN;
}

INT::~INT(){}

DOUBLE::DOUBLE() {
    this->DOUBLE_RANGE.max = LLONG_MAX;
    this->DOUBLE_RANGE.min = LLONG_MIN;
}

DOUBLE::~DOUBLE(){}

FLOAT::FLOAT() {
    this->FLOAT_RANGE.max = INT_MAX;
    this->FLOAT_RANGE.min = INT_MIN;
}

FLOAT::~FLOAT(){}

CHAR::CHAR(unsigned int size) {
    this->CHAR_RANGE.max = size;
    this->CHAR_RANGE.min = size;
}

CHAR::CHAR(){}
CHAR::~CHAR(){}

VARCHAR::VARCHAR(unsigned int size) {
    this->VARCHAR_RANGE.max = size;
    this->VARCHAR_RANGE.min = 0;
}

VARCHAR::VARCHAR(){}
VARCHAR::~VARCHAR(){}

YEAR::YEAR() {
    this->YEAR_RANGE.max = 9999;
    this->YEAR_RANGE.min = 0;
}

YEAR::~YEAR(){}

TIME::TIME() {
    this->TIME_HOUR_RANGE.max = 23;
    this->TIME_HOUR_RANGE.min = 0;
    this->TIME_MINUTE_RANGE.max = 59;
    this->TIME_MINUTE_RANGE.min = 0;
    this->TIME_SECOND_RANGE.max = 59;
    this->TIME_SECOND_RANGE.min = 0;
    this->second = -1;
    this->minute = -1;
    this->hour = -1;
}

TIME::~TIME() {}

DATE::DATE() {
    this->DATE_YEAR_RANGE.max = 9999;
    this->DATE_YEAR_RANGE.min = 0;
    this->day = -1;
    this->month = -1;
    this->year = -1;
}
DATE::~DATE() {}

static int a[12] = {31,28,31,30,31,30,31,31,30,31,30,31};
int DATE::DATE_CHECK() {
    if ((this->year % 100 != 0 && this->year % 4 == 0) || (this->year % 400 == 0)) {     // leap year
        a[1] = 29;
    }
    if (this->year < this->DATE_YEAR_RANGE.min || this->year > this->DATE_YEAR_RANGE.max)
        return 1;       // invalid;
    if (this->month <= 0 || this->month > 12)
        return 1;       // invalid
    for (int i = 0 ; i < 12 ; i++) {
        if (this->day > a[this->month] || this->day <= 0)
            return 1;           // invalid
    }
    a[1] = 28;
    return 0;       // valid
}

int INT::INT_CHECK() {
    if (this->val < this->INT_RANGE.min || this->val > this->INT_RANGE.max)
        return 1;       // invalid
    return 0;       // valid
}

int DOUBLE::DOUBLE_CHECK() {
    if (this->val < this->DOUBLE_RANGE.min || this->val > this->DOUBLE_RANGE.max)
        return 1;       // invalid
    return 0;       // valid
}

int FLOAT::FLOAT_CHECK() {
    if (this->val < this->FLOAT_RANGE.min || this->val > this->FLOAT_RANGE.max)
        return 1;       // invalid
    return 0;       // valid
}

int YEAR::YEAR_CHECK() {
    if (this->val < this->YEAR_RANGE.min || this->val > this->YEAR_RANGE.max)
        return 1;       // invalid
    return 0;       // valid
}

int CHAR::CHAR_CHECK() {
    if (this->val.size() < this->CHAR_RANGE.min || this->val.size() > this->CHAR_RANGE.max)
        return 1;       // invalid
    return 0;       // valid
}

int VARCHAR::VARCHAR_CHECK() {
    if (this->val.size() < this->VARCHAR_RANGE.min || this->val.size() > this->VARCHAR_RANGE.max)
        return 1;       // invalid
    return 0;       // valid
}

int TIME::TIME_CHECK() {
    if (this->second < this->TIME_SECOND_RANGE.min || this->second > this->TIME_SECOND_RANGE.max)
        return 1;       // invalid
    if (this->minute < this->TIME_MINUTE_RANGE.min || this->minute > this->TIME_MINUTE_RANGE.max)
        return 1;       // invalid
    if (this->hour < this->TIME_HOUR_RANGE.min || this->hour > this->TIME_HOUR_RANGE.max)
        return 1;       // invalid
    return 0;       // valid
}





