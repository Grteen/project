#include "common.h"
#include <string>

#define TBTYPEMAX   64      // the maxnum of table's type
#define RECORDMAX   65536       // the maxnum of record number
#define TYPESEP     '\n'        // the sepator of every types in the file
#define ATTRSEP     ','         // the sepator of attribute of types in the file 
#define RECORDSEP   ":"         // the sepator of records in the file
#define RECORDEND   ";"         // the endflag of records in the file

class table_structure{
public:
    int type_num;           // the number of this table's type
    std::string tb_type[TBTYPEMAX];         // the types of this table
    std::string tb_tpname[TBTYPEMAX];       // the name of types
    std::string tb_tpcomment[TBTYPEMAX];        // the comment of types
    std::string tb_comment;         // the comment of this table
    std::string tb_tpattr[TBTYPEMAX];       // the attribute of types
}; 

class table {
public:
    table_structure tb_struct;      // table's structure 
    std::string record[RECORDMAX];
    int record_num;         // the number of this table's records
};