#pragma once
#include "common.h"
#include <iostream>
#include <algorithm>
#include "db_table.h"
#include "DDL.h"
#include "BPT.h"
#include <unordered_map>

using namespace std;

#define IDXMPSEP    ":"     // the sepator in idxmp

// show all index of current table(CUR_TB)
string * show_index(string tb_name , int * size);

// create an index
int create_index(table * tb , string index_field);

// create a B+ tree         // -1 erro 0 success 1 not find
int create_bpt(string tb_name , string index_field);

// delete a index           // -1 error 0 success 1 not find
int drop_index(string tb_name , string index_field);

// change the index string to correct format
string chg_idx(string tb_name , string index_field);