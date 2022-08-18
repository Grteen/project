#pragma once
#include "common.h"
#include <string>
#include <iostream>
#include <unordered_map>
#include "db_table.h"
#include "BPT.h"
#include "index.h"

#define DTATBASEL "data"        // the dir of databases
#define DBMAX   64      // the maxnum of databases.
#define BUFLEN  1024   // the maxbuflength of buffer

#define IF_NOT_EXIST    1   

#define IF_EXIST    1

/* 
    databases DDL
*/

// show all the databases
std::string * show_databases(int * size);       

// show current database
std::string show_cur_db(void);

// create a database   return -1 means error 1 means db exists (no IF_NOT_EXIST) 0 means success
int create_db(std::string db_name , int CDB_flags);

// delete a database   return -1 means error 1 means db not exists (no IF_EXIST) 0 means success
int delete_db(std::string db_name , int DDB_flags);

// change the current database    return -1 means error 1 means db not exists 0 means success
int use_db(std::string db_name);

/*
    tables DDL
*/

#define TBMAX   1024

// show all tables in this databases
std::string * show_tables(int * size);

// create table    return -1 means error 1 means tb exists 0 means success
int create_tb(std::string tb_name , table_structure tb_struct);

// check whether the target table exists  return -1 means error 0 means success 1 means not exists
int check_tb_exist(std::string tb_name);

// show the types in target table
table * desc_tb(std::string tb_name);

// read all information about a table
table * read_tables(std::string tb_name);

// write all table's information to files
int write_tables(table * tb);

// add new field   -1 means error 0 means success 1 means same typename
int add_field(std::string tb_name , std::string newfield , std::string type , std::string comment ,
              std::string attr);

// modify field    -1 means error 0 means success 1 means not find target 2 means have index
int modify_field(std::string tb_name , std::string oldfield , std::string type);

// change field    -1 means error 0 means success 1 means not find target 2 means have index
int change_field(std::string tb_name , std::string oldfield , std::string newfield , std::string type ,
                 std::string comment , std::string attr);

// drop field      -1 means error 0 means success 1 means not find target 2 means have index
int drop_field(std::string tb_name , std::string oldfield);

// rename table    -1 means error 0 means success 1 means not find target
int rename_table(std::string old_tb_name , std::string new_tb_name);

// drop table
int drop_table(std::string tb_name , int DTB_flags);
