#include "DDL.h"

FILE * io_logfp;
std::string CUR_DB;         // current database
std::string CUR_TB;         // current table
char * CUR_DIR;         // current dir

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
        delete []all_db_name;
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
    if (DDB_flags & IF_EXIST)
        return 0;       // success
    fprintf(io_logfp , "delete db : not find target db\n");
    delete []all_db_name;
    return 1;       // not found
}

int use_db(std::string db_name) {
    int size = 0;
    std::string * all_db_name = show_databases(&size);       // get all the db name
    if (all_db_name == NULL) {
        delete []all_db_name;
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
    DIR             *dp;
    struct dirent   *dirp;
    struct stat     statbuf;
    int i = 0;
    std::string * res = new std::string[TBMAX];
    char targetpath[PATH_MAX];

    strcpy(targetpath , "./" DTATBASEL "/");
    strcpy(&targetpath[strlen(targetpath)] , (char *)CUR_DB.data());
    
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

    fclose(fp);
    delete []all_tb_name;
    return 0;       // success
}

table * desc_tb(std::string tb_name) {
    table * res = new table();
    
    res->tb_struct.tb_tpname[0] = "Field";
    res->tb_struct.tb_tpname[1] = "Type";
    res->tb_struct.tb_tpname[2] = "NULL";       // allow_NULL
    res->tb_struct.tb_tpname[3] = "Key";
    res->tb_struct.tb_tpname[4] = "Extra";

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
    while (fgets(buf , 4096 , fp) != NULL) {
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
    while (fgets(buf , 4096 , fp) != NULL) {
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
    while (fgets(buf , 4096 , fp) != NULL) {
        buf[strlen(buf) - 1] = 0;
        res->record[i].append(buf);
        res->record[i++].append(RECORDEND);
    }
    res->tb_struct.type_num = 5;
    res->record_num = i;
    fclose(fp);
    return res;
}

int main(void) {
    io_logfp = fopen(IOLOG , "w+");        // open the IO log
    setvbuf(io_logfp , NULL , _IOLBF , 0);
    create_db("tdb" , 0);
    use_db("tdb");
    table_structure st;
    st.tb_tpname[0] = "1";
    st.tb_tpname[1] = "2";
    st.tb_tpname[2] = "3";
    st.type_num = 3;
    create_tb("ttb" , st);
    table * tb = desc_tb("ttb");
    for (int i = 0 ; i < tb->record_num ; i++) {
        std::cout << tb->record[i] << std::endl;
    }
}