#include "index.h"

extern std::string CUR_DB;         // current database
extern FILE * io_logfp;
std::unordered_map<string , BPtree *> idxmp;            // index map records the field's index

string * show_index(string tb_name , int * size) {
    char fullpath[PATH_MAX];
    struct dirent *dirp;
    struct stat statbuf;
    DIR *dp;
    char *ptr;
    int n;
    string * res = new string[TBTYPEMAX];
    int idxnum = 0;

    strcpy(fullpath , "./" DTATBASEL "/");
    strcpy(&fullpath[strlen(fullpath)] , (char *)CUR_DB.data());
    strcpy(&fullpath[strlen(fullpath)] , "/");
    strcpy(&fullpath[strlen(fullpath)] , (char *)tb_name.data());
    strcpy(&fullpath[strlen(fullpath)] , "/");

    if (lstat(fullpath , &statbuf) < 0)
        fprintf(io_logfp , "show_index : lstat error\n");
    if (S_ISDIR(statbuf.st_mode) == 0)      // not a directory
        fprintf(io_logfp , "show_index : %s not a directory\n" , fullpath);
    
    if ((dp = opendir(fullpath)) == NULL)
        fprintf(io_logfp , "show_index : opendir error\n");
    while ((dirp = readdir(dp)) != NULL) {
        if (strcmp(dirp->d_name , ".") == 0 || strcmp(dirp->d_name , "..") == 0)
            continue;
        if ((ptr = strchr(dirp->d_name , '.')) == NULL)
            continue;       // not find '.'

        // if suffix is ".idx" this file is the index file;
        if (strcmp(ptr , ".idx") == 0) {
            res[idxnum++] = string().assign(dirp->d_name);
        }
    }

    *size = idxnum;

    return res;
}

int create_index(table * tb , string index_field) {
    bool find = false;
    int size = 0;
    int typei = 0;
    string * tb_name = show_tables(&size);

    for (int i = 0 ; i < size ; i++) {
        if (tb->tb_name == tb_name[i]) {        // find the matched ones
            find = true;
        }
    }
    if (!find) {        // not find the matched table_name
        delete []tb_name;
        fprintf(io_logfp , "create_index : tb_name not find name : %s\n" , (char *)tb->tb_name.data());
        return 1;      // not find
    }

    find = false;
    for (int i = 0 ; i < tb->tb_struct.type_num ; i++) {
        if (tb->tb_struct.tb_tpname[i] == index_field) {       // find the matched ones
            find = true;
            typei = i;
        }
    }
    if (!find) {
        delete []tb_name;
        fprintf(io_logfp , "create_index : index_field not find name : %s\n" , (char *)index_field.data());
        return 1;      // not find
    }

    // build index
    char fullpath[PATH_MAX];
    strcpy(fullpath , "./" DTATBASEL "/");
    strcpy(&fullpath[strlen(fullpath)] , (char *)CUR_DB.data());
    strcpy(&fullpath[strlen(fullpath)] , "/");
    strcpy(&fullpath[strlen(fullpath)] , (char *)tb->tb_name.data());
    strcpy(&fullpath[strlen(fullpath)] , "/");
    strcpy(&fullpath[strlen(fullpath)] , (char *)index_field.data());
    strcpy(&fullpath[strlen(fullpath)] , ".idx");

    FILE * fp;
    if ((fp = fopen(fullpath , "w+")) == NULL) {
        fprintf(io_logfp , "create_index : fopen error\n");
        fclose(fp);
        delete []tb_name;
        return -1;      // error
    }

    setvbuf(fp , NULL , _IOLBF , 0);
    for (int i = 0 ; i < tb->record_num ; i++) {
        // index don't allow null
        if (tb->prorecord[i][typei].empty() || tb->prorecord[i][typei] == "NULL") {
            fprintf(io_logfp , "create_index : has NULL value name : %s\n" , (char *)index_field.data());
        }
        fprintf(fp , "%d%s%s%ld%s" , 0 , (char *)tb->prorecord[i][typei].data() , 
                INDEXSEP , tb->recordoff[i] , INDEXEND);
    }

    return 0;       // success
}

int drop_index(string tb_name , string index_field) {
    int size = 0;
    string * res = show_index(tb_name , &size);
    bool find = false;
    string temp = index_field;
    string idx_str;

    temp.append(".idx");
    for (int i = 0 ; i < size ; i++) {
        if (res[i] == temp) {        // find
            find = true;
            break;
        }
    }
    if (!find) {
        fprintf(io_logfp , "drop_index : index_field not found name : %s\n" , (char *)index_field.data());
        delete []res;
        return 1;       // not find
    }

    idx_str = chg_idx(tb_name , index_field);

    auto it = idxmp.find(idx_str);
    if (it != idxmp.end()) {       // the bpt has been built
        // delete it
        it->second->destory_tree();
        idxmp.erase(idx_str);
    }

    char fullpath[PATH_MAX];
    strcpy(fullpath , "./" DTATBASEL "/");
    strcpy(&fullpath[strlen(fullpath)] , (char *)CUR_DB.data());
    strcpy(&fullpath[strlen(fullpath)] , "/");
    strcpy(&fullpath[strlen(fullpath)] , (char *)tb_name.data());
    strcpy(&fullpath[strlen(fullpath)] , "/");
    strcpy(&fullpath[strlen(fullpath)] , (char *)index_field.data());
    strcpy(&fullpath[strlen(fullpath)] , ".idx");

    if (remove(fullpath) != 0) {
        fprintf(io_logfp , "drop_index : remove error\n");
        delete []res;
        return -1;      // error
    }
    
    return 0;       // success
}

// the index_field must be INT  NOT NULL and Unique
int create_bpt(string tb_name , string index_field) {
    BPtree * tree = new BPtree();
    int size = 0;
    bool find = false;
    char fullpath[PATH_MAX];
    FILE * fp;
    char buf[BUFLEN];
    string value[2];        // index value and offset
    int valuei = 0;
    off_t endoff , staoff;
    string temp = index_field;
    string idx_str;

    temp.append(".idx");
    string * res = show_index(tb_name , &size);
    for (int i = 0 ; i < size ; i++) {
        if (res[i] == temp) {
            find = true;
            break;
        }
    }
    if (!find) {
        delete tree;
        delete []res;
        fprintf(io_logfp , "create_bpt : index_field not found name : %s\n" , (char *)index_field.data());
        return 1;       // not find
    }

    strcpy(fullpath , "./" DTATBASEL "/");
    strcpy(&fullpath[strlen(fullpath)] , (char *)CUR_DB.data());
    strcpy(&fullpath[strlen(fullpath)] , "/");
    strcpy(&fullpath[strlen(fullpath)] , (char *)tb_name.data());
    strcpy(&fullpath[strlen(fullpath)] , "/");
    strcpy(&fullpath[strlen(fullpath)] , (char *)index_field.data());
    strcpy(&fullpath[strlen(fullpath)] , ".idx");

    if ((fp = fopen(fullpath , "r+")) == NULL) {
        delete tree;
        delete []res;
        fclose(fp);
        fprintf(io_logfp , "create_bpt : fopen error\n");
        return -1;      // error
    }

    setvbuf(fp , NULL , _IOLBF , 0);
    staoff = 0;
    while (fgets(buf , BUFLEN , fp) != NULL) {
        buf[strlen(buf) - 1] = 0;
        if (buf[0] == '0') {        // valid
            endoff = ftell(fp);
            value[0] = "";
            value[1] = "";
            for (int i = 1 ; i < strlen(buf) ; i++) {
                if (buf[i] != ':')
                    value[valuei] = value[valuei] + buf[i];
                else 
                    valuei++;
            }
            valuei = 0;
        }
        else            // invalid
            continue;
        tree->insert_Node(stoll(value[0]) , stoll(value[1]) , staoff);
        staoff = endoff;
    }

    idx_str = chg_idx(tb_name , index_field);

    idxmp.insert(make_pair(idx_str , tree));
    return 0;       // success
}

string chg_idx(string tb_name , string index_field) {
    string idx_str;

    idx_str.append(CUR_DB);
    idx_str.append(IDXMPSEP);
    idx_str.append(tb_name);
    idx_str.append(IDXMPSEP);
    idx_str.append(index_field);

    return idx_str;
}

// int main() {
//     io_logfp = fopen("./log/IO.log" , "w+");
//     use_db("tdb");
//     table * tb = read_tables("6");
//     create_index(tb , "1name");
//     create_bpt("6" , "1name");
//     auto it = idxmp.find(chg_idx("6" , "1name"));
//     if (it != idxmp.end())
//         cout << it->second->root->key[0]->val << endl;
// }