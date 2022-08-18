#include "DQL.h"

extern FILE * io_logfp;
extern std::string CUR_DB;         // current database
extern std::unordered_map<string , BPtree *> idxmp;            // index map records the field's index


record * read_record(string tb_name , off_t off , int * index , int indexnum) {
    if (check_tb_exist(tb_name) != 0)
        return NULL;
    FILE * fp;

    char fullpath[PATH_MAX];
    char buf[BUFLEN];
    string temp[TBTYPEMAX];
    int tempi = 0;
    record * res = new record();

    strcpy(fullpath , "./" DTATBASEL);
    strcpy(&fullpath[strlen(fullpath)] , "/");
    strcpy(&fullpath[strlen(fullpath)] , (char *)CUR_DB.data());
    strcpy(&fullpath[strlen(fullpath)] , "/");
    strcpy(&fullpath[strlen(fullpath)] , (char *)tb_name.data());
    strcpy(&fullpath[strlen(fullpath)] , "/");
    strcpy(&fullpath[strlen(fullpath)] , "records");

    if ((fp = fopen(fullpath , "r+")) == NULL) {
        fclose(fp);
        fprintf(io_logfp , "read_record : fopen error\n");
        delete res;
        return NULL;
    }

    setvbuf(fp , NULL , _IOLBF , 0);
    fseek(fp , off , SEEK_SET);
    fgets(buf , BUFLEN , fp);

    // read the record and deal with to the correct format
    for (int i = 0 ; i < strlen(buf) ; i++) {
        if (buf[i] == ':') {
            tempi++;
            continue;
        }
        else {
            temp[tempi] = temp[tempi] + buf[i];
        }
    }

    for (int i = 0 ; i < indexnum ; i++) {
        res->fields[res->fieldsnum++] = temp[index[i]];
    }

    fclose(fp);
    return res;
}

void put_rec(table * tb , record * rec) {
    for (int i = 0 ; i < tb->tb_struct.type_num ; i++) {
        tb->prorecord[tb->record_num][i] = rec->value[i];
    }
    tb->record_num++;
}

table * select_record(string * fields , string * alias , int fieldsnum , string tb_name , int DQL_FLAG , 
                      string sel_field , string min , string max) 
{
    if (check_tb_exist(tb_name) != 0)
        return NULL;

    table * res = new table();
    FILE * fp;
    char buf[BUFLEN];
    int k = 0;
    int indexnum = 0;
    int * index = new int(TBTYPEMAX);

    res->tb_struct.type_num = fieldsnum;
    for (int i = 0 ; i < fieldsnum ; i++)
        res->tb_struct.tb_tpname[i] = alias[i];

    char fullpath[PATH_MAX];
    int i = 0;
    strcpy(fullpath , "./" DTATBASEL);
    strcpy(&fullpath[strlen(fullpath)] , "/");
    strcpy(&fullpath[strlen(fullpath)] , (char *)CUR_DB.data());
    strcpy(&fullpath[strlen(fullpath)] , "/");
    strcpy(&fullpath[strlen(fullpath)] , (char *)tb_name.data());
    strcpy(&fullpath[strlen(fullpath)] , "/");
    strcpy(&fullpath[strlen(fullpath)] , "typenames");
    
    if ((fp = fopen(fullpath , "r+")) == NULL) {
        fclose(fp);
        delete res;
        fprintf(io_logfp , "select_record : fopen error\n");
    }
    setvbuf(fp , NULL , _IOLBF , 0);
    while (fgets(buf , BUFLEN , fp) != NULL) {
        buf[strlen(buf) - 1] = 0;
        for (int i = 0 ; i < fieldsnum ; i++) {
            if (fields[i] == string().assign(buf)) {
                index[indexnum++] = k;      // the index of type that equal to fields
                break;
            }
        }
        k++;
    }

    auto it = idxmp.find(chg_idx(tb_name , sel_field));
    if (it != idxmp.end()) {        // this field has the index
        In_Node * temp;
        record * rec;
        if (DQL_FLAG == DQL_EQUAL) {        // if condition is equal
            temp = it->second->search_Sim(it->second->root , stoll(min));     // find the record
            // read the target record
            rec = read_record(tb_name , temp->off , index , indexnum);  
            put_rec(res , rec);
            delete rec;
        }
        else if (DQL_FLAG == DQL_RANGE) {   // if condition is range
            temp = it->second->search_Node(it->second->root , stoll(min));     // find the record
            rec = read_record(tb_name , temp->off , index , indexnum);   // the min
            int nodei = 0;
            // find this record's index location
            for (int i = 0 ; i < temp->master->keynum ; i++) {    
                if (temp == temp->master->key[i]) {
                    nodei = i;
                }
            }
            Node * Ntemp = temp->master;

            // goto BPtree's right to find others which satify the condition
            while (Ntemp != NULL) {
                for (int i = nodei ; i < Ntemp->keynum ; i++) {         
                    if (Ntemp->key[i]->val <= stoll(max)) {
                        rec = read_record(tb_name , Ntemp->key[i]->off , index , indexnum);
                        put_rec(res , rec);
                    }
                    else {
                        goto finish;
                    }

                    if (i == Ntemp->keynum - 1) {
                        Ntemp = Ntemp->Next_Node;
                        nodei = 0;
                    }
                }
            }

            finish:
                ;
        }

        delete temp;
        delete rec;
        return res;       // success
    }

    // this field doesn't have index
    table * all_record = read_tables(tb_name);      // read all records
    int sel_field_index = 0;
    for (int i = 0 ; i < all_record->tb_struct.type_num ; i++) {
        if (all_record->tb_struct.tb_tpname[i] == sel_field) {
            sel_field_index = i;
            break;
        }
    }
    record * rec = new record();
    int t = 0;
    for (int i = 0 ; i < all_record->record_num ; i++) {
        // satify the condition
        if (stoll(min) <= stoll(all_record->prorecord[i][sel_field_index]) 
            && stoll(all_record->prorecord[i][sel_field_index]) <= stoll(max)) {
            for (int k = 0 ; k < indexnum ; k++) {
                rec->value[t++] = all_record->prorecord[i][index[k]];
            }
            put_rec(res , rec);
        }
    }

    delete rec;
    return res;     // success
}

int main(void) {
    io_logfp = fopen("./log/IO.log" , "w+");
    use_db("tdb");
    record rcd;
    rcd.fieldsnum = 2;
    rcd.fields[0] = "1name";
    rcd.fields[1] = "3name";
    rcd.value[0] = "30";
    rcd.value[1] = "250";

    record rcd2;
    rcd2.fieldsnum = 2;
    rcd2.fields[0] = "1name";
    rcd2.fields[1] = "3name";
    rcd2.value[0] = "25";
    rcd2.value[1] = "200";

    record rcd3;
    rcd3.fieldsnum = 2;
    rcd3.fields[0] = "1name";
    rcd3.fields[1] = "3name";
    rcd3.value[0] = "40";
    rcd3.value[1] = "500";

    table * tb = read_tables("6");
    create_index(tb , "1name");
    create_bpt("6" , "1name");
    insert_record("6" , rcd);
    insert_record("6" , rcd2);
    insert_record("6" , rcd3);

    string fields[100];
    fields[0] = "1name";
    string ailas[100];
    ailas[0] = "ailas";
    string max = "40";
    string min = "30";

    tb = select_record(fields , ailas , 1 , "6" , DQL_EQUAL , "1name" , max , min);
    cout << tb->prorecord[0][0] << endl;
}