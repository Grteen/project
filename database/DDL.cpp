#include "DDL.h"

extern FILE * io_logfp;
std::string CUR_DB = "";         // current database
char * CUR_DIR;         // current dir
extern std::unordered_map<string , BPtree *> idxmp;            // index map records the field's index

/* 
    databases DDL
*/

std::string * show_databases(int * size) {
    DIR             *dp;
    struct dirent   *dirp;
    struct stat     statbuf;
    int i = 0;
    std::string * res = new std::string[DBMAX];
    
    if ((dp = opendir(DTATBASEL)) == NULL) {      // can't read directory
        fprintf(io_logfp , "show_databases : can't read dir" DTATBASEL "\n");
        return NULL;
    }
    while ((dirp = readdir(dp)) != NULL) {
        if (strcmp(dirp->d_name , ".") == 0 || strcmp(dirp->d_name , "..") == 0)
            continue;           // ignore dot and dot-dot
        res[i++].assign(dirp->d_name);
    }
    *size = i;
    return res;
}

std::string show_cur_db(void) {
    return CUR_DB;
}

int create_db(std::string db_name , int CDB_flags) {
    int size = 0;
    std::string * all_db_name = show_databases(&size);       // get all the db name
    if (all_db_name == NULL)
        return -1;      // error
    for (int i = 0 ; i < size ; i++){
        if (all_db_name[i] == db_name){
            if (CDB_flags & IF_NOT_EXIST) {       // have the IF_NOT_EXIST flags
                delete []all_db_name;
                return 0;           // success;
            }
            else {            // no IF_NOT_EXIST and find a db'name as same as target db
                delete []all_db_name;
                fprintf(io_logfp , "create_db : db exists name : %s\n" , (char*)db_name.data());
                return 1;           // db exists
            }
        }
    }
    char fullpath[PATH_MAX];
    strcpy(fullpath , "./" DTATBASEL); 
    strcpy(&fullpath[strlen(fullpath)] , "/");
    strcpy(&fullpath[strlen(fullpath)] , (char *)db_name.data());

    if (mkdir(fullpath , O_RDWR | 0777) == -1) {
        fprintf(io_logfp , "create_db : mkdir error\n");
        delete []all_db_name;
        return -1;      // error
    }
    fprintf(io_logfp , "create_db : create db success name : %s" , (char *)db_name.data());
    delete []all_db_name;
    return 0;       // success
}

int delete_db(std::string db_name , int DDB_flags) {
    FILE * fp;
    int size = 0;
    std::string * all_db_name = show_databases(&size);       // get all the db name
    if (all_db_name == NULL) {
        return -1;          // error
    }
    for (int i = 0 ; i < size ; i++){
        if (all_db_name[i] == db_name){             // find the target
            char cmdstr[PATH_MAX];
            strcpy(cmdstr , "rm -rf ./");
            strcpy(&cmdstr[strlen(cmdstr)] , DTATBASEL "/");
            strcpy(&cmdstr[strlen(cmdstr)] , (char *)db_name.data());
            if ((fp = popen(cmdstr , "r")) == NULL) {
                fprintf(io_logfp , "delete db : popen error\n");
                delete []all_db_name;
                return -1;      // error
            }
            fprintf(io_logfp , "delete_db success name : %s\n" , (char *)db_name.data());
            delete []all_db_name;
            return 0;       // success
        }
    }
    // not find target db
    if (DDB_flags & IF_EXIST) {
        fprintf(io_logfp , "delete db : not find target db\n");
        delete []all_db_name;
        fclose(fp);
        return 0;       // success
    }
    fclose(fp);
    fprintf(io_logfp , "delete db : not find target db\n");
    delete []all_db_name;
    return 1;       // not found
}

int check_db_exist(std::string db_name) {
    int size = 0;
    std::string * all_db_name = show_databases(&size);       // get all the db name
    if (all_db_name == NULL) {
        fprintf(io_logfp , "use_db : show_databases error");
        return -1;      // error
    }
    for (int i = 0 ; i < size ; i++){
        if (all_db_name[i] == db_name){     // find the target db
            delete []all_db_name;
            return 0;           // success
        }
    }

    delete []all_db_name;
    return 1;       // not find
}

int use_db(std::string db_name) {
    int size = 0;
    std::string * all_db_name = show_databases(&size);       // get all the db name
    if (all_db_name == NULL) {
        fprintf(io_logfp , "use_db : show_databases error");
        return -1;      // error
    }
    for (int i = 0 ; i < size ; i++){
        if (all_db_name[i] == db_name){     // find the target db
            CUR_DB = db_name;           // change the current db
            fprintf(io_logfp , "use_db : use db success name : %s\n" , (char *)db_name.data());
            delete []all_db_name;
            return 0;           // success
        }
    }
    fprintf(io_logfp , "use_db : use db failed not find target db name : %s\n" , (char *)db_name.data());
    delete []all_db_name;
    return 1;       // target db not find
}

/* 
    tables DDL
*/

std::string * show_tables(int * size) {
    if (CUR_DB == "")
        return NULL;
    DIR             *dp;
    struct dirent   *dirp;
    struct stat     statbuf;
    int i = 0;
    std::string * res = new std::string[TBMAX];
    char targetpath[PATH_MAX];

    strcpy(targetpath , "./" DTATBASEL "/");
    strcpy(&targetpath[strlen(targetpath)] , (char *)CUR_DB.data());
    strcpy(&targetpath[strlen(targetpath)] , "/");
    
    if ((dp = opendir(targetpath)) == NULL) {      // can't read directory
        fprintf(io_logfp , "show_tables : can't read dir %s\n" , (char *)CUR_DB.data());
        return NULL;
    }
    while ((dirp = readdir(dp)) != NULL) {
        if (strcmp(dirp->d_name , ".") == 0 || strcmp(dirp->d_name , "..") == 0)
            continue;           // ignore dot and dot-dot
        res[i++].assign(dirp->d_name);
    }
    *size = i;
    return res;
}

int create_tb(std::string tb_name , table_structure tb_struct) {
    int size = 0;
    std::string * all_tb_name = show_tables(&size);       // get all the tb name
    if (all_tb_name == NULL)
        return -1;      // error
    for (int i = 0 ; i < size ; i++){
        if (all_tb_name[i] == tb_name){         // find the same name
            fprintf(io_logfp , "create_tb : table exists name : %s\n" , (char *)tb_name.data());
            delete []all_tb_name;
            return 1;         // same name
        }
    }
    char fullpath[PATH_MAX];
    strcpy(fullpath , "./" DTATBASEL);
    strcpy(&fullpath[strlen(fullpath)] , "/");
    strcpy(&fullpath[strlen(fullpath)] , (char *)CUR_DB.data());
    strcpy(&fullpath[strlen(fullpath)] , "/");
    strcpy(&fullpath[strlen(fullpath)] , (char *)tb_name.data());

    if (mkdir(fullpath , O_RDWR | 0777) == -1) {
        fprintf(io_logfp , "create_db : mkdir error\n");
        delete []all_tb_name;
        return -1;      // error
    }
    fprintf(io_logfp , "create_db : create db success name : %s\n" , (char *)tb_name.data());
    strcpy(&fullpath[strlen(fullpath)] , "/");
    char temppath[PATH_MAX];
    strcpy(temppath , fullpath);
    strcpy(&fullpath[strlen(fullpath)] , "types");      // the type of this table
    FILE *fp;
    if ((fp = fopen(fullpath , "w+")) == NULL) {
        fprintf(io_logfp , "create_db : fopen error\n");
        fclose(fp);
        delete []all_tb_name;
        return -1;      // error
    }
    setvbuf(fp , NULL , _IOLBF , 0);
    for (int i = 0 ; i < tb_struct.type_num ; i++) {
        fprintf(fp , "%s%c" , (char *)tb_struct.tb_type[i].data() , TYPESEP);
    }

    memset(fullpath , 0 , sizeof(fullpath));
    strcpy(fullpath , temppath);
    
    strcpy(&fullpath[strlen(fullpath)] , "typenames");      // the name of type
    if ((fp = fopen(fullpath , "w+")) == NULL) {
        fprintf(io_logfp , "create_db : fopen error\n");
        fclose(fp);
        delete []all_tb_name;
        return -1;      // error
    }
    setvbuf(fp , NULL , _IOLBF , 0);
    for (int i = 0 ; i < tb_struct.type_num ; i++) {
        fprintf(fp , "%s%c" , (char *)tb_struct.tb_tpname[i].data() , TYPESEP);
    }

    memset(fullpath , 0 , sizeof(fullpath));
    strcpy(fullpath , temppath);

    strcpy(&fullpath[strlen(fullpath)] , "typecomments");      // the comment of type
    if ((fp = fopen(fullpath , "w+")) == NULL) {
        fprintf(io_logfp , "create_db : fopen error\n");
        fclose(fp);
        delete []all_tb_name;
        return -1;      // error
    }
    setvbuf(fp , NULL , _IOLBF , 0);
    for (int i = 0 ; i < tb_struct.type_num ; i++) {
        fprintf(fp , "%s%c" , (char *)tb_struct.tb_tpcomment[i].data() , TYPESEP);
    }

    memset(fullpath , 0 , sizeof(fullpath));
    strcpy(fullpath , temppath);

    strcpy(&fullpath[strlen(fullpath)] , "tablecomment");      // the comment of table
    if ((fp = fopen(fullpath , "w+")) == NULL) {
        fprintf(io_logfp , "create_db : fopen error\n");
        fclose(fp);
        delete []all_tb_name;
        return -1;      // error
    }
    setvbuf(fp , NULL , _IOLBF , 0);
    fprintf(fp , "%s%c" , (char *)tb_struct.tb_comment.data() , TYPESEP);

    memset(fullpath , 0 , sizeof(fullpath));
    strcpy(fullpath , temppath);

    strcpy(&fullpath[strlen(fullpath)] , "typeattrs");      // the attribute of type
    if ((fp = fopen(fullpath , "w+")) == NULL) {
        fprintf(io_logfp , "create_db : fopen error\n");
        fclose(fp);
        delete []all_tb_name;
        return -1;      // error
    }
    setvbuf(fp , NULL , _IOLBF , 0);
    for (int i = 0 ; i < tb_struct.type_num ; i++) {
        fprintf(fp , "%s%c" , (char *)tb_struct.tb_tpattr[i].data() , TYPESEP);
    }

    memset(fullpath , 0 , sizeof(fullpath));
    strcpy(fullpath , temppath);

    strcpy(&fullpath[strlen(fullpath)] , "records");      // the records in this table
    if ((fp = fopen(fullpath , "w+")) == NULL) {
        fprintf(io_logfp , "create_db : fopen error\n");
        fclose(fp);
        delete []all_tb_name;
        return -1;      // error
    }
    setvbuf(fp , NULL , _IOLBF , 0);
    fclose(fp);
    delete []all_tb_name;
    return 0;       // success
}

int check_tb_exist(std::string tb_name) {
    int size = 0;
    std::string * tb = show_tables(&size);
    for (int i = 0 ; i < size ; i++) {
        if (tb[i] == tb_name) {
            return 0;       // success
        }
    }
    fprintf(io_logfp , "check_tb_exist : target table not exists name : %s" , (char *)tb_name.data());
    return 1;       // failed
}

table * desc_tb(std::string tb_name) {
    if (check_tb_exist(tb_name) != 0)
        return NULL;
    table * res = new table();
    
    res->tb_struct.tb_tpname[0] = "Field";
    res->tb_struct.tb_tpname[1] = "Type";
    res->tb_struct.tb_tpname[2] = "attribute";

    char fullpath[PATH_MAX];
    strcpy(fullpath , "./" DTATBASEL);
    strcpy(&fullpath[strlen(fullpath)] , "/");
    strcpy(&fullpath[strlen(fullpath)] , (char *)CUR_DB.data());
    strcpy(&fullpath[strlen(fullpath)] , "/");
    strcpy(&fullpath[strlen(fullpath)] , (char *)tb_name.data());
    strcpy(&fullpath[strlen(fullpath)] , "/");

    char temppath[PATH_MAX];
    strcpy(temppath , fullpath);
    strcpy(&fullpath[strlen(fullpath)] , "typenames");

    FILE * fp;
    char buf[BUFLEN];

    if ((fp = fopen(fullpath , "r+")) == NULL) {
        fprintf(io_logfp , "desc_db : fopen error\n");
        fclose(fp);
        delete res;
        return NULL;        // error
    }
    setvbuf(fp , NULL , _IOLBF , 0);
    int i = 0;
    while (fgets(buf , BUFLEN , fp) != NULL) {
        buf[strlen(buf) - 1] = 0;
        res->record[i].append(buf);
        res->record[i++].append(RECORDSEP);
    }
    
    strcpy(fullpath , temppath);
    strcpy(&fullpath[strlen(fullpath)] , "types");
    if ((fp = fopen(fullpath , "r+")) == NULL) {
        fprintf(io_logfp , "desc_db : fopen error\n");
        fclose(fp);
        delete res;
        return NULL;        // error
    }
    
    setvbuf(fp , NULL , _IOLBF , 0);
    i = 0;
    while (fgets(buf , BUFLEN , fp) != NULL) {
        buf[strlen(buf) - 1] = 0;
        res->record[i].append(buf);
        res->record[i++].append(RECORDSEP);
    }

    strcpy(fullpath , temppath);
    strcpy(&fullpath[strlen(fullpath)] , "typeattrs");
    if ((fp = fopen(fullpath , "r+")) == NULL) {
        fprintf(io_logfp , "desc_db : fopen error\n");
        fclose(fp);
        delete res;
        return NULL;        // error
    }
    setvbuf(fp , NULL , _IOLBF , 0);
    i = 0;
    while (fgets(buf , BUFLEN , fp) != NULL) {
        buf[strlen(buf) - 1] = 0;
        res->record[i].append(buf);
        res->record[i++].append(RECORDEND);
    }
    res->tb_struct.type_num = 3;
    res->record_num = i;
    res->process_record();
    fclose(fp);
    return res;
}


table * read_tables(std::string tb_name) {
    if (check_tb_exist(tb_name) != 0)
        return NULL;

    table * res = new table();
    char fullpath[PATH_MAX];
    int i = 0;
    strcpy(fullpath , "./" DTATBASEL);
    strcpy(&fullpath[strlen(fullpath)] , "/");
    strcpy(&fullpath[strlen(fullpath)] , (char *)CUR_DB.data());
    strcpy(&fullpath[strlen(fullpath)] , "/");
    strcpy(&fullpath[strlen(fullpath)] , (char *)tb_name.data());
    strcpy(&fullpath[strlen(fullpath)] , "/");
    
    char temppath[PATH_MAX];
    strcpy(temppath , fullpath);
    strcpy(&fullpath[strlen(fullpath)] , "typenames");

    res->tb_name = tb_name;
    FILE * fp;
    char buf[BUFLEN];

    if ((fp = fopen(fullpath , "r+")) == NULL) {
        fprintf(io_logfp , "read_tables : fopen error\n");
        fclose(fp);
        delete res;
        return NULL;        // error
    }
    i = 0;
    setvbuf(fp , NULL , _IOLBF , 0);
    while (fgets(buf , BUFLEN , fp) != NULL) {
        buf[strlen(buf) - 1] = 0;
        res->tb_struct.tb_tpname[i++].assign(buf);
    }

    strcpy(fullpath , temppath);
    strcpy(&fullpath[strlen(fullpath)] , "types");

    if ((fp = fopen(fullpath , "r+")) == NULL) {
        fprintf(io_logfp , "read_tables : fopen error\n");
        fclose(fp);
        delete res;
        return NULL;        // error
    }
    i = 0;
    setvbuf(fp , NULL , _IOLBF , 0);
    while (fgets(buf , BUFLEN , fp) != NULL) {
        buf[strlen(buf) - 1] = 0;
        res->tb_struct.tb_type[i++].assign(buf);
    }
    res->tb_struct.type_num = i;

    strcpy(fullpath , temppath);
    strcpy(&fullpath[strlen(fullpath)] , "typecomments");

    if ((fp = fopen(fullpath , "r+")) == NULL) {
        fprintf(io_logfp , "read_tables : fopen error\n");
        fclose(fp);
        delete res;
        return NULL;        // error
    }
    i = 0;
    setvbuf(fp , NULL , _IOLBF , 0);
    while (fgets(buf , BUFLEN , fp) != NULL) {
        buf[strlen(buf) - 1] = 0;
        res->tb_struct.tb_tpcomment[i++].assign(buf);
    }

    strcpy(fullpath , temppath);
    strcpy(&fullpath[strlen(fullpath)] , "typeattrs");

    if ((fp = fopen(fullpath , "r+")) == NULL) {
        fprintf(io_logfp , "read_tables : fopen error\n");
        fclose(fp);
        delete res;
        return NULL;        // error
    }
    i = 0;
    setvbuf(fp , NULL , _IOLBF , 0);
    while (fgets(buf , BUFLEN , fp) != NULL) {
        buf[strlen(buf) - 1] = 0;
        res->tb_struct.tb_tpattr[i++].assign(buf);
    }

    strcpy(fullpath , temppath);
    strcpy(&fullpath[strlen(fullpath)] , "tablecomment");

    if ((fp = fopen(fullpath , "r+")) == NULL) {
        fprintf(io_logfp , "read_tables : fopen error\n");
        fclose(fp);
        delete res;
        return NULL;        // error
    }
    setvbuf(fp , NULL , _IOLBF , 0);
    while (fgets(buf , BUFLEN , fp) != NULL) {
        buf[strlen(buf) - 1] = 0;
        res->tb_struct.tb_comment.assign(buf);
    }

    strcpy(fullpath , temppath);
    strcpy(&fullpath[strlen(fullpath)] , "records");
    if ((fp = fopen(fullpath , "r+")) == NULL) {
        fprintf(io_logfp , "read_tables : fopen error\n");
        fclose(fp);
        delete res;
        return NULL;        // error
    }
    i = 0;
    setvbuf(fp , NULL , _IOLBF , 0);
    res->recordoff[0] = 0;
    while (fgets(buf , BUFLEN , fp) != NULL) {
        buf[strlen(buf) - 1] = 0;
        res->recordoff[i + 1] = ftell(fp);
        if (buf[0] != '0')       // invalid
            continue;
        res->record[i++].assign(&buf[2]);
    }
    res->record_num = i;
    res->process_record();
    res->tb_struct.process_attr();

    fclose(fp);
    return res;
}

int write_tables(table * tb) {
    if (check_tb_exist(tb->tb_name) != 0)
        return 1;
    char fullpath[PATH_MAX];
    int i = 0;
    strcpy(fullpath , "./" DTATBASEL);
    strcpy(&fullpath[strlen(fullpath)] , "/");
    strcpy(&fullpath[strlen(fullpath)] , (char *)CUR_DB.data());
    strcpy(&fullpath[strlen(fullpath)] , "/");
    // The task of deleting and adding folders is handed over to the upper layer
    strcpy(&fullpath[strlen(fullpath)] , (char *)tb->tb_name.data());
    strcpy(&fullpath[strlen(fullpath)] , "/");

    char temppath[PATH_MAX];
    strcpy(temppath , fullpath);
    strcpy(&fullpath[strlen(fullpath)] , "typenames");
    FILE * fp;
    char buf[BUFLEN];

    if ((fp = fopen(fullpath , "w+")) == NULL) {
        fprintf(io_logfp , "write_tables : fopen error\n");
        fclose(fp);
        return -1;        // error
    }
    i = 0;
    setvbuf(fp , NULL , _IOLBF , 0);
    for (int i = 0 ; i < tb->tb_struct.type_num ; i++) {
        fprintf(fp , "%s\n" , (char *)tb->tb_struct.tb_tpname[i].data());
    }

    strcpy(fullpath, temppath);
    strcpy(&fullpath[strlen(fullpath)] , "types");

    if ((fp = fopen(fullpath , "w+")) == NULL) {
        fprintf(io_logfp , "write_tables : fopen error\n");
        fclose(fp);
        return -1;        // error
    }
    i = 0;
    setvbuf(fp , NULL , _IOLBF , 0);
    for (int i = 0 ; i < tb->tb_struct.type_num ; i++)
        fprintf(fp , "%s\n" ,(char *)tb->tb_struct.tb_type[i].data());
    
    strcpy(fullpath, temppath);
    strcpy(&fullpath[strlen(fullpath)] , "typeattrs");

    if ((fp = fopen(fullpath , "w+")) == NULL) {
        fprintf(io_logfp , "write_tables : fopen error\n");
        fclose(fp);
        return -1;        // error
    }
    i = 0;
    setvbuf(fp , NULL , _IOLBF , 0);
    for (int i = 0 ; i < tb->tb_struct.type_num ; i++)
        fprintf(fp , "%s\n" , (char *)tb->tb_struct.tb_tpattr[i].data());

    strcpy(fullpath, temppath);
    strcpy(&fullpath[strlen(fullpath)] , "typecomments");

    if ((fp = fopen(fullpath , "w+")) == NULL) {
        fprintf(io_logfp , "write_tables : fopen error\n");
        fclose(fp);
        return -1;        // error
    }
    i = 0;
    setvbuf(fp , NULL , _IOLBF , 0);
    for (int i = 0 ; i < tb->tb_struct.type_num ; i++)
        fprintf(fp , "%s\n" , (char *)tb->tb_struct.tb_tpcomment[i].data());

    strcpy(fullpath, temppath);
    strcpy(&fullpath[strlen(fullpath)] , "tablecomment");

    if ((fp = fopen(fullpath , "w+")) == NULL) {
        fprintf(io_logfp , "write_tables : fopen error\n");
        fclose(fp);
        return -1;        // error
    }
    i = 0;
    setvbuf(fp , NULL , _IOLBF , 0);
    fprintf(fp , "%s\n" , (char *)tb->tb_struct.tb_comment.data());

    strcpy(fullpath, temppath);
    strcpy(&fullpath[strlen(fullpath)] , "records");

    if ((fp = fopen(fullpath , "w+")) == NULL) {
        fprintf(io_logfp , "write_tables : fopen error\n");
        fclose(fp);
        return -1;        // error
    }
    i = 0;
    setvbuf(fp , NULL , _IOLBF , 0);
    for (int i = 0 ; i < tb->record_num ; i++) {
        int length = 0;
        length = strlen((char *)tb->record[i].data());
        // valid_bit record_length record newline
        fprintf(fp , "%c%s%s\n" , '0' , RECORDSEP , (char *)tb->record[i].data());
    }

    fclose(fp);
    return 0;       // success
}

int add_field(std::string tb_name , std::string newfield , std::string type , std::string comment ,
             std::string attr) 
{
    if (check_tb_exist(tb_name) != 0)
        return 1;
    if (check_filed_type(type) != 0)
        return 3;
    table *tb = new table();
    if ((tb = read_tables(tb_name)) == NULL) {
        fprintf(io_logfp , "add_field : read_tables error name : %s" , (char *)tb_name.data());
        delete tb;
        return -1;      // error
    }

    for (int i = 0 ; i < tb->tb_struct.type_num ; i++)
        if (tb->tb_struct.tb_tpname[i] == newfield) {    // find the same one
            fprintf(io_logfp , "add_field : find the same name of types name : %s" , (char *)newfield.data());
            delete tb;
            return 2;       // failed
        }
    
    // change the information of table
    int num = tb->tb_struct.type_num;
    tb->tb_struct.tb_type[num] = type;
    tb->tb_struct.tb_tpname[num] = newfield;
    tb->tb_struct.tb_tpcomment[num] = comment;
    tb->tb_struct.tb_tpattr[num] = attr;
    tb->tb_struct.type_num++;

    // process the record into easy mode
    tb->process_record();
    // check data if satify new type
    // tb->check_data_validity();
    // change the record
    tb->write_record();

    // write table information to file
    if (write_tables(tb) == -1) {
        fprintf(io_logfp , "add_field : write_tables error name : %s" , (char *)tb_name.data());
        delete tb;
        return -1;      // error
    }
    delete tb;
    return 0;
}

int modify_field(std::string tb_name , std::string oldfield , std::string type) {
    // this oldfield have index
    if (idxmp.find(chg_idx(tb_name , oldfield)) != idxmp.end()) {
        fprintf(io_logfp , "modify_field : oldfield has index name : %s" , (char *)oldfield.data());
        return 2;       // have index;
    }
    if (check_tb_exist(tb_name) != 0)
        return 1;
    if (check_filed_type(type) != 0)
        return 3;
    table *tb = new table();
    bool find_flag = false;
    if ((tb = read_tables(tb_name)) == NULL) {
        fprintf(io_logfp , "modify_field : read_tables error name : %s" , (char *)tb_name.data());
        delete tb;
        return -1;      // error
    }

    for (int i = 0 ; i < tb->tb_struct.type_num ; i++) {
        if (tb->tb_struct.tb_tpname[i] == oldfield) {       // find the target field
            tb->tb_struct.tb_type[i] = type;        // change the type
            find_flag = true;
            break;
        }
    }

    if (!find_flag) {
        fprintf(io_logfp , "modify_field : not find target typename name : %s" , (char *)oldfield.data());
        delete tb;
        return 1;       // failed
    }

    // process the record into easy mode
    tb->process_record();
    // check data if satify new type
    // tb->check_data_validity();
    // change the record
    tb->write_record();

    // write table information to file
    if (write_tables(tb) == -1) {
        fprintf(io_logfp , "modify_field : write_tables error name : %s" , (char *)tb_name.data());
        delete tb;
        return -1;
    }
    delete tb;
    return 0;
}

int change_field(std::string tb_name , std::string oldfield , std::string newfield , std::string type ,
                 std::string comment , std::string attr) 
{
    if (idxmp.find(chg_idx(tb_name , oldfield)) != idxmp.end()) {
        fprintf(io_logfp , "change_field : oldfield has index name : %s" , (char *)oldfield.data());
        return 2;       // have index;
    }

    if (check_filed_type(type) != 0)
        return 3;

    if (check_tb_exist(tb_name) != 0)
        return 1;
    table *tb = new table();
    bool find_flag = false;
    if ((tb = read_tables(tb_name)) == NULL) {
        fprintf(io_logfp , "change_field : read_tables error name : %s" , (char *)tb_name.data());
        delete tb;
        return -1;      // error
    }
    // find oldfield

    // check newfield 
    for (int i = 0 ; i < tb->tb_struct.type_num ; i++) {
        if (tb->tb_struct.tb_tpname[i] == newfield) {
            fprintf(io_logfp , "change field : find the same name of types name : %s" , (char *)newfield.data());
            delete tb;
            return 4;       // same fields
        }
    }
    for (int i = 0 ; i < tb->tb_struct.type_num ; i++) {
        if (tb->tb_struct.tb_tpname[i] == oldfield) {
            find_flag = true;
            // change information
            tb->tb_struct.tb_tpname[i] = newfield;
            tb->tb_struct.tb_type[i] = type;
            tb->tb_struct.tb_tpcomment[i] = comment;
            tb->tb_struct.tb_tpattr[i] = attr;
            break;
        }
    }

    if (!find_flag) {
        fprintf(io_logfp , "change_field : not find target typename name : %s" , (char *)oldfield.data());
        delete tb;
        return 1;       // failed
    }

    // process the record into easy mode
    tb->process_record();
    // check data if satify new type
    // tb->check_data_validity();
    // change the record
    tb->write_record();

    // write table information to file
    if (write_tables(tb) == -1) {
        fprintf(io_logfp , "change_field : write_tables error name : %s" , (char *)tb_name.data());
        delete tb;
        return -1;
    }
    
    delete tb;
    return 0;       // success
}

int drop_field(std::string tb_name , std::string oldfield) {
    if (check_tb_exist(tb_name) != 0)
        return 1;
    if (idxmp.find(chg_idx(tb_name , oldfield)) != idxmp.end()) {
        fprintf(io_logfp , "drop_field : oldfield has index name : %s" , (char *)oldfield.data());
        return 2;       // have index;
    }

    if (check_tb_exist(tb_name) != 0)
        return 0;
    table *tb = new table();
    bool find_flag = false;
    if ((tb = read_tables(tb_name)) == NULL) {
        fprintf(io_logfp , "drop_field : read_tables error name : %s" , (char *)tb_name.data());
        delete tb;
        return -1;      // error
    }
    // find oldfield
    for (int i = 0 ; i < tb->tb_struct.type_num ; i++) {
        if (tb->tb_struct.tb_tpname[i] == oldfield) {
            find_flag = true;
            // delete it 
            for (int j = i ; j < tb->tb_struct.type_num ; j++) {
                tb->tb_struct.tb_tpname[j] = tb->tb_struct.tb_tpname[j + 1];   
                tb->tb_struct.tb_type[j] = tb->tb_struct.tb_type[j + 1];  
                tb->tb_struct.tb_tpattr[j] = tb->tb_struct.tb_tpattr[j + 1];  
                tb->tb_struct.tb_comment[j] = tb->tb_struct.tb_comment[j + 1]; 
            }
            // put the final ones NULL
            tb->tb_struct.tb_type[tb->tb_struct.type_num - 1] = "";     
            tb->tb_struct.tb_tpname[tb->tb_struct.type_num - 1] = "";     
            tb->tb_struct.tb_tpattr[tb->tb_struct.type_num - 1] = "";     
            tb->tb_struct.tb_tpcomment[tb->tb_struct.type_num - 1] = "";     
            tb->tb_struct.type_num--;
            break;
        }
    }

    if (!find_flag) {
        fprintf(io_logfp , "drop_field : not find target typename name : %s" , (char *)oldfield.data());
        delete tb;
        return 1;       // failed
    }

    // process the record into easy mode
    tb->process_record();
    // check data if satify new type
    // tb->check_data_validity();
    // change the record
    tb->write_record();

    // write table information to file
    if (write_tables(tb) == -1) {
        fprintf(io_logfp , "drop_field : write_tables error name : %s" , (char *)tb_name.data());
        delete tb;
        return -1;
    }

    delete tb;
    return 0;       // success
}

int rename_table(std::string old_tb_name , std::string new_tb_name) {  
    int size = 0;
    std::string * tb = show_tables(&size);
    char fullpath[PATH_MAX];
    char newpath[PATH_MAX];

    for (int i = 0 ; i < size ; i++) {
        if (tb[i] == old_tb_name) {       // find the target
            strcpy(fullpath , "./" DTATBASEL "/");
            strcpy(&fullpath[strlen(fullpath)] , (char *)CUR_DB.data());
            strcpy(&fullpath[strlen(fullpath)] , "/");

            strcpy(newpath , fullpath);
            strcpy(&fullpath[strlen(fullpath)] , (char *)tb[i].data());
            strcpy(&newpath[strlen(newpath)] , (char *)new_tb_name.data());
            if (rename(fullpath , newpath) == -1) {
                fprintf(io_logfp , "rename_table : rename error , oldname : %s , newname : %s" ,
                        (char *)old_tb_name.data() , (char *)new_tb_name.data());
                delete []tb;
                return -1;      // error
            }
            return 0;       // success
        } 
    }
    delete []tb;
    return 1;       // failed
}

int drop_table(std::string tb_name , int DTB_flags) {
    FILE * fp;
    int size = 0;
    std::string * tb = show_tables(&size);       // get all the db name
    for (int i = 0 ; i < size ; i++){
        if (tb[i] == tb_name){             // find the target
            char cmdstr[PATH_MAX];
            strcpy(cmdstr , "rm -rf ./");
            strcpy(&cmdstr[strlen(cmdstr)] , DTATBASEL "/");
            strcpy(&cmdstr[strlen(cmdstr)] , (char *)CUR_DB.data());
            strcpy(&cmdstr[strlen(cmdstr)] , "/");
            strcpy(&cmdstr[strlen(cmdstr)] , (char *)tb_name.data());
            std::cout << cmdstr << std::endl;
            if ((fp = popen(cmdstr , "r")) == NULL) {
                fprintf(io_logfp , "delete db : popen error\n");
                fclose(fp);
                delete []tb;
                return -1;      // error
            }
            fprintf(io_logfp , "delete_db success name : %s\n" , (char *)tb_name.data());
            delete []tb;
            return 0;       // success
        }
    }
    // not find target db
    if (DTB_flags & IF_EXIST) {
        fprintf(io_logfp , "delete db : not find target db\n");
        delete []tb;
        return 0;       // success
    }
    fprintf(io_logfp , "delete db : not find target db\n");
    delete []tb;
    return 1;       // not found
}

int check_filed_type(std::string field) {
    if (field == "INT" || field == "FLOAT" || field == "DOUBLE" ||
        field == "YEAR" || field == "TIME" || field == "DATE")
        return 0;       // success
    return 1;      // failed
}