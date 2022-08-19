#pragma once
#include "common.h"
#include "index.h"
#include "db_table.h"
#include "DDL.h"
#include "DML.h"
#include <iostream>
#include <unordered_map>
#include <string>

using namespace std;

#define  DQL_EQUAL      0       // equal condition
#define  DQL_RANGE      1       // range condition

// select the all the records which satify the condition
table * select_record(string * fields , string * alias , int fieldsnum , string tb_name , int DQL_FLAG , 
                      string sel_field , string min , string max);

// read record
record * read_record(string tb_name , off_t off , int * index , int indexnum);

// put record into table
void put_rec(table * tb , record * rec);