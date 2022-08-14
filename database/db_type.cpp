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
}

TIME::~TIME() {}

DATE::DATE() {
    this->DATE_YEAR_RANGE.max = 9999;
    this->DATE_YEAR_RANGE.min = 0;
}
DATE::~DATE() {}

static int a[12] = {31,28,31,30,31,30,31,31,30,31,30,31};
int DATE_checkfunction(DATE date) {
    if ((date.year % 100 != 0 && date.year % 4 == 0) || (date.year % 400 == 0)) {     // leap year
        a[1] = 29;
    }
    if (date.year < date.DATE_YEAR_RANGE.min || date.year > date.DATE_YEAR_RANGE.max)
        return 1;       // invalid;
    if (date.month <= 0 || date.month > 12)
        return 1;       // invalid
    for (int i = 0 ; i < 12 ; i++) {
        if (date.day > a[date.month] || date.day <= 0)
            return 1;           // invalid
    }
    a[1] = 28;
    return 0;       // valid
}
