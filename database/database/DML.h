#include "common.h"
#include "index.h"
#include "DDL.h"
#include <iostream>
#include <string>
#include <unordered_map>

using namespace std;

class record {
public:
    int fieldsnum;
    string fields[TBTYPEMAX];
    string value[TBTYPEMAX];
};

// write value to index file    -1 error 0 success
int write_index(string tb_name , string field , string key , off_t off , off_t * staloc);

// insert record        -1 error 0 success 1 not find
int insert_record(string tb_name , record rcd);