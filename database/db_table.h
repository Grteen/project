#pragma once
#include "common.h"
#include <string>
#include "db_type.h"

#define TBTYPEMAX   64      // the maxnum of table's type
#define RECORDMAX   65536       // the maxnum of record number
#define TBTYATTRMAX 8           // the maxnum of attribute of types
#define TYPESEP     '\n'        // the sepator of every types in the file
#define ATTRSEP     ','         // the sepator of attribute of types in the file 
#define RECORDSEP   ":"         // the sepator of records in the file
#define RECORDEND   "\n"         // the endflag of records in the file
#define INDEXSEP    ":"         // the sepator of index in file
#define INDEXEND    "\n"        // the endflag of index in file

class table_structure{
public:
    int type_num;           // the number of this table's type
    std::string tb_type[TBTYPEMAX];         // the types of this table
    std::string tb_tpname[TBTYPEMAX];       // the name of types
    std::string tb_tpcomment[TBTYPEMAX];        // the comment of types
    std::string tb_comment;         // the comment of this table
    std::string tb_tpattr[TBTYPEMAX];       // the attribute of types
    std::string tb_proattr[TBTYPEMAX][TBTYATTRMAX];         // processed attributes
    int tb_attrnum[TBTYPEMAX];          // the number of attribute of every types

    void process_attr();        // process attribute
    void write_attr();          // accord to proattr to write tpattr
}; 

class table {
public:
    table_structure tb_struct;      // table's structure 
    std::string record[RECORDMAX];
    off_t recordoff[RECORDMAX];     // offset of every record
    int record_num;         // the number of this table's records
    std::string tb_name;        // the name of table
    std::string prorecord[RECORDMAX][TBTYPEMAX];        // processed records

    void process_record();      // process records
    void check_data_validity();         // if invalid , put NULL
    void write_record();        // accord to prorecord to write record
};