#include "DML.h"

extern std::unordered_map<string , BPtree *> idxmp;            // index map records the field's index
extern std::string CUR_DB;         // current database
extern FILE * io_logfp;
extern In_Node *p;

int write_index(string tb_name , string field , string key , off_t off , off_t * staloc) {
    char fullpath[PATH_MAX];
    FILE * fp;
    char buf[BUFSIZ];

    strcpy(fullpath , "./" DTATBASEL "/");
    strcpy(&fullpath[strlen(fullpath)] , (char *)CUR_DB.data());
    strcpy(&fullpath[strlen(fullpath)] , "/");
    strcpy(&fullpath[strlen(fullpath)] , (char *)tb_name.data());
    strcpy(&fullpath[strlen(fullpath)] , "/");
    strcpy(&fullpath[strlen(fullpath)] , (char *)field.data());
    strcpy(&fullpath[strlen(fullpath)] , ".idx");

    if ((fp = fopen(fullpath , "a+")) == NULL) {
        fprintf(io_logfp , "write_index : fopen error\n");
        return -1;      // error
    }
    setvbuf(fp , buf , _IOLBF , BUFSIZ);
    fseek(fp , 0 , SEEK_END);       // go to the end of file to get the offset
    if (staloc != NULL)
        *staloc = ftell(fp);     // get the start offset of this index record
    fprintf(fp , "%d%s%s%ld%s" , 0 , (char *)key.data() , INDEXSEP , off , INDEXEND);
    fclose(fp);
    return 0;       // succeess
}

int delete_index(string tb_name , string field , off_t off) {
    char fullpath[PATH_MAX];
    FILE * fp;

    strcpy(fullpath , "./" DTATBASEL "/");
    strcpy(&fullpath[strlen(fullpath)] , (char *)CUR_DB.data());
    strcpy(&fullpath[strlen(fullpath)] , "/");
    strcpy(&fullpath[strlen(fullpath)] , (char *)tb_name.data());
    strcpy(&fullpath[strlen(fullpath)] , "/");
    strcpy(&fullpath[strlen(fullpath)] , (char *)field.data());
    strcpy(&fullpath[strlen(fullpath)] , ".idx");

    if ((fp = fopen(fullpath , "r+")) == NULL) {
        fclose(fp);
        fprintf(io_logfp , "write_index : fopen error\n");
        return -1;      // error
    }
    setvbuf(fp , NULL , _IONBF , 0);
    fseek(fp , off , SEEK_CUR);     // set the fp to target off

    fprintf(fp , "%s" , "1");         // make this index invalid
    fclose(fp);

    return 0;       // success
}

int delete_record(string tb_name , off_t off) {
    char fullpath[PATH_MAX];
    char buf[BUFLEN];
    FILE * fp;

    strcpy(fullpath , "./" DTATBASEL "/");
    strcpy(&fullpath[strlen(fullpath)] , (char *)CUR_DB.data());
    strcpy(&fullpath[strlen(fullpath)] , "/");
    strcpy(&fullpath[strlen(fullpath)] , (char *)tb_name.data());
    strcpy(&fullpath[strlen(fullpath)] , "/records");

    if ((fp = fopen(fullpath , "r+")) == NULL) {
        fclose(fp);
        fprintf(io_logfp , "delete_record : fopen error\n");
        return -1;      // error
    }
    setvbuf(fp , NULL , _IONBF , 0);
    fseek(fp , off , SEEK_SET);     // set the fp to target off

    fprintf(fp , "%s" , "1");         // make this record invalid
    fclose(fp);

    return 0;       // success
}

int insert_record(string tb_name , record rcd) {
    if (check_tb_exist(tb_name) != 0)
        return 1;
    char fullpath[PATH_MAX];
    int size = 0;
    FILE * fp;
    off_t offset;
    string * all_index;
    off_t staloc;

    strcpy(fullpath , "./" DTATBASEL "/");
    strcpy(&fullpath[strlen(fullpath)] , (char *)CUR_DB.data());
    strcpy(&fullpath[strlen(fullpath)] , "/");
    strcpy(&fullpath[strlen(fullpath)] , (char *)tb_name.data());
    strcpy(&fullpath[strlen(fullpath)] , "/");
    strcpy(&fullpath[strlen(fullpath)] , "records");

    if ((fp = fopen(fullpath , "a+")) == NULL) {
        fprintf(io_logfp , "write_tables : fopen error\n");
        return -1;        // error
    }

    setvbuf(fp , NULL , _IOLBF , 0);
    fseek(fp , 0 , SEEK_END);       // go to the end of file to get the offset
    offset = ftell(fp);
    fprintf(fp , "%d%s" , 0 , RECORDSEP);
    for (int i = 0 ; i < rcd.fieldsnum ; i++) {
        if (i != rcd.fieldsnum - 1)
            fprintf(fp , "%s%s" , (char *)rcd.value[i].data() , RECORDSEP);
        else
            fprintf(fp , "%s%s" , (char *)rcd.value[i].data() , RECORDEND);
    }

    all_index = show_index(tb_name , &size);
    for (int i = 0 ; i < rcd.fieldsnum ; i++) {
        for (int k = 0 ; k < size ; k++) {
            string temp = rcd.fields[i];
            temp.append(".idx");
            if (temp == all_index[k]) {          // find the associated index file
                write_index(tb_name, rcd.fields[i], rcd.value[i], offset, &staloc);
                break;
            }
        }
        // put the offset to all the B+ tree
        auto it = idxmp.find(chg_idx(tb_name , rcd.fields[i]));
        if (it != idxmp.end())
            it->second->insert_Node(stoll(rcd.value[i]) , offset , staloc);
    }

    delete []all_index;
    fclose(fp);
    fprintf(io_logfp , "insert a new record to table %s\n" , (char *)tb_name.data());
    return 0;       // success
}

int update_record(string tb_name , record oldrcd , record newrcd , int DML_FLAG) {
    if (check_tb_exist(tb_name) != 0)
        return 1;

    char fullpath[PATH_MAX];
    char temppath[PATH_MAX];
    string * all_index;
    int size = 0;
    bool drop_data = false;
    off_t idx_off = 0;

    all_index = show_index(tb_name , &size);

    for (int i = 0 ; i < oldrcd.fieldsnum ; i++) {
        for (int k = 0 ; k < size ; k++) {
            string temp = oldrcd.fields[i];
            temp.append(".idx");
            if (temp == all_index[k]) {          // find the associated index file
                auto it = idxmp.find(chg_idx(tb_name , oldrcd.fields[i]));
                if (it != idxmp.end()) {
                    // get this index offset in index file
                    In_Node * temp = it->second->search_Node(it->second->root , stoll(oldrcd.value[i]));
                    idx_off = temp->idxoff;
                    if (!drop_data) {
                        if (delete_record(tb_name , temp->off) == -1)     // delete the record
                            return -1;      // error
                        drop_data = true;
                    }
                    it->second->remove_Node(stoll(oldrcd.value[i]));        // remove this index in BPtree
                    // delete this index in index file
                    if (delete_index(tb_name , oldrcd.fields[i] , idx_off) == -1)
                        return -1; 
                }
            }
        }
    }

    // no index
    if (!drop_data) {
        if (delete_record(tb_name , idx_off) == -1)     // delete the record
            return -1;      // error
        drop_data = true;
    }

    if (DML_FLAG == DML_UPDATE) {
        if (insert_record(tb_name , newrcd) == -1)
            return -1;      // error
    }

    return 0;       // success
}

record * read_fields(string tb_name) {
    record * rcd = new record();

    char fullpath[PATH_MAX];
    strcpy(fullpath , "./" DTATBASEL "/");
    strcpy(&fullpath[strlen(fullpath)] , (char *)CUR_DB.data());
    strcpy(&fullpath[strlen(fullpath)] , "/");
    strcpy(&fullpath[strlen(fullpath)] , (char *)tb_name.data());
    strcpy(&fullpath[strlen(fullpath)] , "/");
    strcpy(&fullpath[strlen(fullpath)] , "typenames");
    FILE * fp;
    if ((fp = fopen(fullpath , "r+")) == NULL) {
        delete rcd;
        fprintf(io_logfp , "read_fields : fopen error\n");
        return NULL;
    }

    int fieldi = 0;
    char buf[BUFLEN];
    while (fgets(buf , BUFLEN , fp) != NULL) {
        buf[strlen(buf) - 1] = 0;
        rcd->fields[fieldi++] = string().assign(buf);
    }
    rcd->fieldsnum = fieldi;
    fclose(fp);
    return rcd;
}