#include "DML.h"

extern std::unordered_map<string , BPtree *> idxmp;            // index map records the field's index
extern std::string CUR_DB;         // current database
extern FILE * io_logfp;

int write_index(string tb_name , string field , string key , off_t off , off_t * staloc) {
    char fullpath[PATH_MAX];
    FILE * fp;

    strcpy(fullpath , "./" DTATBASEL "/");
    strcpy(&fullpath[strlen(fullpath)] , (char *)CUR_DB.data());
    strcpy(&fullpath[strlen(fullpath)] , "/");
    strcpy(&fullpath[strlen(fullpath)] , (char *)tb_name.data());
    strcpy(&fullpath[strlen(fullpath)] , "/");
    strcpy(&fullpath[strlen(fullpath)] , (char *)field.data());
    strcpy(&fullpath[strlen(fullpath)] , ".idx");

    if ((fp = fopen(fullpath , "a+")) == NULL) {
        fclose(fp);
        fprintf(io_logfp , "write_index : fopen error\n");
        return -1;      // error
    }
    setvbuf(fp , NULL , _IOLBF , 0);
    fseek(fp , 0 , SEEK_END);       // go to the end of file to get the offset

    *staloc = ftell(fp);     // get the start offset of this index record
    fprintf(fp , "%d%s%s%ld%s" , 0 , (char *)key.data() , INDEXSEP , off , INDEXEND);
    return 0;       // succeess
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
        fclose(fp);
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

    fprintf(io_logfp , "insert a new record to table %s\n" , (char *)tb_name.data());
    return 0;       // success
}

int main(void) {
    io_logfp = fopen("./log/IO.log" , "w+");
    use_db("tdb");
    record rcd;
    rcd.fieldsnum = 2;
    rcd.fields[0] = "1name";
    rcd.fields[1] = "3name";
    rcd.value[0] = "30";
    rcd.value[1] = "200";
    table * tb = read_tables("6");
    create_index(tb , "1name");
    create_bpt("6" , "1name");
    insert_record("6" , rcd);
    auto it = idxmp.find(chg_idx("6" , "1name"));
    if (it != idxmp.end())
         cout << it->second->root->key[0]->val << endl;

}