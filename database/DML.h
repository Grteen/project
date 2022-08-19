#pragma once
#include "common.h"
#include "index.h"
#include "DDL.h"
#include <iostream>
#include <string>
#include <unordered_map>

using namespace std;

#define DML_DROP        1
#define DML_UPDATE      0

class record {
public:
    int fieldsnum;
    string fields[TBTYPEMAX];
    string value[TBTYPEMAX];
};

// write value to index file    -1 error 0 success
int write_index(string tb_name , string field , string key , off_t off , off_t * staloc);

// delete index in index file   -1 error 0 success
int delete_index(string tb_name , string field , off_t off);

// delete record in data file   -1 error 0 success
int delete_record(string tb_name , off_t off);

// insert record        -1 error 0 success 1 not find
int insert_record(string tb_name , record rcd);

// update or drop record        -1 error 0 success 1 not find
int update_record(string tb_name , record oldrcd , record newrcd , int DML_FLAG);