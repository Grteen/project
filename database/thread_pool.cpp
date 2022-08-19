#include "thread_pool.h"

extern FILE * logfp;
extern std::string CUR_DB;         // current database

std::condition_variable cond;

threadinfo::threadinfo() {}
threadinfo::~threadinfo() {}

void Prosql(threadinfo * thrinfo) {
    int res = 0;
    while (1) {
        std::unique_lock<std::mutex> locker(thrinfo->thrmu , std::defer_lock);
        locker.lock();
        thrinfo->change_free(true);         // free
        cond.wait(locker , [=](){return thrinfo->get_allow();});        // wait tasks
        locker.unlock();
        thrinfo->change_free(false);        // busy 
        // process function ...
        if ((res = thrinfo->analyzer()) != 0)
            send(thrinfo->get_fd() , (char *)returnval[res].data() , returnval[res].size() , 0);
        thrinfo->change_allow(false);           // process done , so next time will be not allowed and waiting to be changed
    }
}

void threadinfo::init() {
    this->_allow = false; 
    this->_argsize = thread_arg;
    this->_free = true;
}

bool threadinfo::get_allow() {
    return this->_allow;
}

int threadinfo::change_allow(bool allow) {
    this->_allow = allow;
    return 0;       // change success
}

int threadinfo::change_sql(std::string sql) {
    this->_sql = sql;
    return 0;       // change sucess
}

std::string threadinfo::get_sql() {
    return this->_sql;
}

bool threadinfo::get_free() {
    return this->_free;
}

int threadinfo::change_free(bool free) {
    this->_free = free;
    return 1;       // change success
}

int threadinfo::get_fd() {
    return this->_fd;
}

int threadinfo::change_fd(int fd) {
    this->_fd = fd;
    return 0;       // change success
}

thread_pool::thread_pool() {}
thread_pool::~thread_pool() {}

void thread_pool::init() {
    this->_size = thread_max;
    for (int i = 0 ; i < thread_max ; i++) {
        threadinfo *thr = new threadinfo();
        thr->init();
        this->_thrvec.push_back(thr);
        std::thread t(Prosql , thr);        // push the threadinfo into process function
        t.detach();
    }
}

bool thread_pool::check_free(threadinfo * tf) {
    std::unique_lock<std::mutex> locker(this->_tkmu);
    bool res = tf->get_free();
    return res;
}

void thread_pool::put_into_tasks(task *tk) {
    std::unique_lock<std::mutex> locker(this->_tkmu);
    this->_tasks.push(tk);
}

int thread_pool::get_task(task * target) {
    std::unique_lock<std::mutex> locker(this->_tkmu);
    if (this->_tasks.empty()) 
        return -1;
    *target = *(this->_tasks.back());            // get the task
    this->_tasks.pop();
    return 0;
}

bool thread_pool::get_empty() {
    return this->_tasks.empty();
}

void init_returnval() {
    returnval[0] = "Success\n";    
    returnval[1] = "invalid end_flag : end_flag must be the ';'\n";
    returnval[2] = "invalid sql sepator : multiple sql sepator\n";
    returnval[3] = "invalid end_flag : end_flag must be at the end of a sql\n";
    returnval[4] = "invalid sql : unknown sql\n";
    returnval[5] = "sql failed : unknown database\n";
    returnval[6] = "sql failed : unknown tablename\n";
    returnval[7] = "sql failed : database exists\n";
    returnval[8] = "program error\n";
    returnval[9] = "sql failed : table exists\n";
    returnval[10] = "sql failed : unknown fields\n";
    returnval[11] = "sql failed : same field\n";
    returnval[12] = "sql failed : unknown field type\n";
    returnval[13] = "sql failed : filed has index\n";
    returnval[14] = "sql failed : unknown index\n";
}

int threadinfo::analyzer() {
    string sqlseg[SQLMAX];

    if (this->_sql[this->_sql.size() - 2] != SQLENDFLAG)
        return 1;

    int segnum = 0;

    // deal with the whole sql to the segments
    for (int i = 0 ; i < this->_sql.size(); i++) {
        if (this->_sql[i] == SQLENDFLAG) {
            if (i != this->_sql.size() - 2)
                return 3;
            else 
                break;
        }

        if (this->_sql[i] != SQLSEP)
            sqlseg[segnum] = sqlseg[segnum] + this->_sql[i];
        else {
            if (this->_sql[i + 1] == SQLSEP)
                return 2;
            else
                segnum++;
        }
    }

    segnum++;
    if (sqlseg[0] == "show") {
        return show_sql(sqlseg , segnum);
    }
    else if (sqlseg[0] == "use") {
        return use_sql(sqlseg , segnum);
    }
    else if (sqlseg[0] == "create") {
        return create_sql(sqlseg , segnum);
    }
    else if (sqlseg[0] == "drop") {
        return drop_sql(sqlseg , segnum);
    }
    else if (sqlseg[0] == "desc") {
        return desc_sql(sqlseg , segnum);
    }
    else if (sqlseg[0] == "alter") {
        return alter_sql(sqlseg , segnum);
    }

    return 4;
}

int threadinfo::show_sql(string * sqlseg , int segnum) {
    // show databases;
    if (sqlseg[1] == "databases") {
        if (segnum != 2)
            return 4;
        int size = 0;
        string * res = NULL;
        string msg;

        res = show_databases(&size);
        
        for (int i = 0 ; i < size ; i++) {
            msg.append(res[i]);
            msg.append("\n");
        }

        if (send(this->_fd , (char *)msg.data() , msg.size() , 0) == -1)
            fprintf(logfp , "threadinfo::show_sql : send error\n");
        
        delete []res;
        return 0;
    }
    // show tables
    else if (sqlseg[1] == "tables") {
        if (segnum != 2)
            return 4;
        if (check_db_exist(CUR_DB) != 0) {
            return 5;
        }

        int size = 0;
        string * res = NULL;
        string msg;

        res = show_tables(&size);

        for (int i = 0 ; i < size ; i++) {
            msg.append(res[i]);
            msg.append("\n");
        }

        if (send(this->_fd , (char *)msg.data() , msg.size() , 0) == -1)
            fprintf(logfp , "threadinfo::show_sql : send error\n");
        
        delete []res;
        return 0;
    }
    // show index from tb_name
    else if (sqlseg[1] == "index") {
        if (segnum != 4 && sqlseg[2] != "from")
            return 4;
        string tb_name = sqlseg[3];
        if (check_tb_exist(tb_name) != 0)
            return 6;
        
        string * res = NULL;
        int size = 0;
        string msg;

        res = show_index(tb_name , &size);

        for (int i = 0 ; i < size ; i++) {
            msg.append(res[i]);
            msg.append("\n");
        }

        if (send(this->_fd , (char *)msg.data() , msg.size() , 0) == -1)
            fprintf(logfp , "threadinfo::show_sql : send error\n");

        delete []res;
        return 0;
    }
    
    return 4;
}

int threadinfo::use_sql(string * sqlseg , int segnum) {
    // use db_name
    if (segnum == 2) {
        string db_name = sqlseg[1];
        string msg;
        int res;

        res = use_db(db_name);
        if (res == 1) {
            return 5;
        }
        else if (res == -1) {
            return 8;
        }
        else if (res == 0) {
            msg = "use databases success\n";
            if (send(this->_fd , (char *)msg.data() , msg.size() , 0) == -1)
                fprintf(logfp , "threadinfo::use_sql : send error\n");
            return 0;
        }
    }

    return 4;
}

int threadinfo::create_sql(string * sqlseg , int segnum) {
    // create database [if not exists] db_name
    if (sqlseg[1] == "database") {
        // create database if not exists db_name
        if (segnum == 6 && sqlseg[1] == "database" && sqlseg[2] == "if" && 
            sqlseg[3] == "not" && sqlseg[4] == "exists") 
        {
            string db_name = sqlseg[5];
            int res;
            string msg;

            res = create_db(db_name , IF_NOT_EXIST);
            if (res == -1) {
                return 8;
            }
            else if (res == 0) {
                msg = "create database success\n";
                if (send(this->_fd , (char *)msg.data() , msg.size() , 0) == -1)
                    fprintf(logfp , "threadinfo::create_sql : send error\n");
                return 0;
            }
        }
        // create database db_name
        else if (segnum == 3 && sqlseg[1] == "database") {
            string db_name = sqlseg[2];
            int res;
            string msg;
            res = create_db(db_name , 0);
            if (res == -1) {
                return 8;
            }
            else if (res == 1) {
                return 7;
            }
            else if (res == 0) {
                msg = "create database success\n";
                if (send(this->_fd , (char *)msg.data() , msg.size() , 0) == -1)
                    fprintf(logfp , "threadinfo::create_sql : send error\n");
                return 0;
            }
        }
        
        return 4;
    }
    // create table tb_name (...)
    else if (sqlseg[1] == "table") {
        if (sqlseg[3] == "(") {
            string tb_name = sqlseg[2];
            if (check_tb_exist(tb_name) == 0)
                return 9;
            int i = 4;
            table_structure tb_stu;
            int fieldi = 0;
            int fieldloc = 0;
            while (sqlseg[i] != ")") {
                if (sqlseg[i] == ",") {
                    if (fieldloc == 3)
                        return 4;
                    fieldi++;
                    fieldloc = 0;
                }
                else {
                    if (fieldloc == 0) {
                        tb_stu.tb_tpname[fieldi] = sqlseg[i];
                        fieldloc++;
                    }
                    else if (fieldloc == 1) {
                        string type = sqlseg[i];
                        if (type == "INT" || type == "DOUBLE" || type == "FLOAT" || type == "YEAR"
                            || type == "TIME" || type == "TIME" || type == "DATE")
                        {
                            tb_stu.tb_type[fieldi] = type;
                        }
                        else {
                            if (type[0] == 'C' && type[1] == 'H' && type[2] == 'A' || type[3] == 'R'
                                && type[4] == '(' && type[type.size() - 1] == ')') 
                            {
                                tb_stu.tb_type[fieldi] = type;
                            }
                            else if (type[0] == 'V' && type[1] == 'A' && type[2] == 'R' && type[3] == 'C'
                                     && type[4] == 'C' && type[5] == 'H' && type[6] == 'A' && type[7] == 'R'
                                     && type[8] == '(' && type[type.size() - 1] == ')')
                            {
                                tb_stu.tb_type[fieldi] = type;
                            }

                            return 4;
                        }
                        fieldloc++;
                    }
                    else if (fieldloc == 2) {
                        if (sqlseg[i] != "comment")
                            return 4;
                        fieldloc++;
                    }
                    else if (fieldloc == 3) {
                        string comm = sqlseg[i];
                        if (comm.size() < 2 || comm[0] != '\'' || comm[comm.size() - 1] != '\'') {
                            return 4;
                        }
                        string res = "";
                        for (int i = 1 ; i < comm.size() - 1 ; i++) {
                            res = res + comm[i];
                        }
                        tb_stu.tb_tpcomment[fieldi] = res;
                        fieldloc++;
                    } 
                    else if (fieldloc == 4) {
                        return 4;
                    }
                }
                i++;
            }

            tb_stu.type_num = fieldi + 1;

            if (sqlseg[i + 1] == "comment") {
                string comm = sqlseg[i + 2];
                if (comm.size() < 2 || comm[0] != '\'' || comm[comm.size() - 1] != '\'') {
                    return 4;
                }
                string res = "";
                for (int i = 1 ; i < comm.size() - 1 ; i++)
                    res = res + comm[i];
                tb_stu.tb_comment = res;
            }

            int res = 0;
            res = create_tb(tb_name , tb_stu);
            if (res == -1)
                return 8;
            else if (res == 1)
                return 9;
            else if (res == 0) {
                string msg = "create table success\n";
                if (send(this->_fd , (char *)msg.data() , msg.size() , 0) == -1)
                    fprintf(logfp , "threadinfo::create_sql : send error\n");
                return 0;
            }
        }
        return 4;
    }
    // create index index_name on tb_name (...)
    else if (sqlseg[1] == "index") {
        if (segnum == 8 && sqlseg[3] == "on" && sqlseg[5] == "(" && sqlseg[segnum - 1] == ")") {
            string tb_name = sqlseg[4];
            if (check_tb_exist(tb_name) != 0) 
                return 6;
            table *tb = read_tables(tb_name);
            int res = 0;
            res = create_index(tb , sqlseg[6]);
            delete tb;
            if (res == -1)
                return 8;
            else if (res == 1)
                return 10;
            else if (res == 0) {
                string msg = "create index success\n";
                if (send(this->_fd , (char *)msg.data() , msg.size() , 0) == -1)
                    fprintf(logfp , "threadinfo::create_sql : send error\n");
                return 0;
            }
        }
    }
    return 4;
}

int threadinfo::drop_sql(string * sqlseg , int segnum) {
    // drop database [if exists] db_name
    if (segnum >= 3 && sqlseg[1] == "database") {
        // drop database if exists db_name
        if (segnum == 5 && sqlseg[2] == "if" && sqlseg[3] == "exists") {
            string db_name = sqlseg[4];
            if (check_db_exist(db_name) != 0) {
                string msg = "drop database success\n";
                if (send(this->_fd , (char *)msg.data() , msg.size() , 0) == -1)
                    fprintf(logfp , "threadinfo::drop_sql : send error\n");
                return 0;
            }
            int res = 0;
            res = delete_db(db_name , IF_EXIST);
            if (res == -1)
                return 8;
            else if (res == 0) {
                string msg = "drop database success\n";
                if (send(this->_fd , (char *)msg.data() , msg.size() , 0) == -1)
                    fprintf(logfp , "threadinfo::drop_sql : send error\n");
                return 0;
            }    
        }
        // drop database db_name
        else if (segnum == 3) {
            string db_name = sqlseg[2];
            if (check_db_exist(db_name) != 0)
                return 5;
            int res = 0;
            res = delete_db(db_name , 0);
            if (res == -1)
                return 8;
            else if (res == 1) {
                return 5;
            }
            else if (res == 0) {
                string msg = "drop database success\n";
                if (send(this->_fd , (char *)msg.data() , msg.size() , 0) == -1)
                    fprintf(logfp , "threadinfo::drop_sql : send error\n");
                return 0;
            }
        }
        return 4;
    }
    // drop table [if exists] tb_name
    else if (segnum >= 3 && sqlseg[1] == "table") {
        // drop table if exists tb_name
        if (segnum == 5 && sqlseg[2] == "if" && sqlseg[3] == "exists") {
            string tb_name = sqlseg[4];
            int res = 0;
            res = drop_table(tb_name , IF_EXIST);
            if (res == -1)
                return 8;
            else if (res == 0) {
                string msg = "drop table success\n";
                if (send(this->_fd , (char *)msg.data() , msg.size() , 0) == -1)
                    fprintf(logfp , "threadinfo::drop_sql : send error\n");
                return 0;
            }
        }
        // drop tale tb_name
        else if (segnum == 3) {
            string tb_name = sqlseg[2];
            int res = 0;
            res = drop_table(tb_name , 0);
            if (res == -1)
                return 8;
            else if (res == 1)
                return 6;
            else if (res == 0) {
                string msg = "drop table success\n";
                if (send(this->_fd , (char *)msg.data() , msg.size() , 0) == -1)
                    fprintf(logfp , "threadinfo::drop_sql : send error\n");
                return 0;
            }
        }
        return 4;
    }
    // drop index idx_name on tb_name
    else if (segnum == 5 && sqlseg[1] == "index") {
        string tb_name = sqlseg[4];
        string idx_name = sqlseg[2];
        int res;
        res = drop_index(tb_name , idx_name);
        if (res == -1)
            return 8;
        else if (res == 1) 
            return 14;
        else if (res == 0) {
            string msg = "drop index success\n";
            if (send(this->_fd , (char *)msg.data() , msg.size() , 0) == -1)
                fprintf(logfp , "threadinfo::drop_sql : send error\n");
            return 0;
        }
    }
    return 4;
}

int threadinfo::desc_sql(string * sqlseg , int segnum) {
    // desc tb_name 
    if (segnum == 2) {
        string tb_name = sqlseg[1];
        if (check_tb_exist(tb_name) != 0) 
            return 6;
        table *tb;
        string msg;
        tb = desc_tb(tb_name);
        msg = print_table(tb);
        if (send(this->_fd , (char *)msg.data() , msg.size() , 0) == -1)
            fprintf(logfp , "threadinfo::desc_sql : send error\n");
        return 0;
    }

    return 4;
}

string print_table(table * tb) {
    string res;
    for (int i = 0 ; i < tb->tb_struct.type_num ; i++) {
        res.append(tb->tb_struct.tb_tpname[i]);
        res.append("\t\t");
    }
    res.append("\n");
    for (int i = 0 ; i < tb->record_num ; i++) {
        for (int k = 0 ; k < tb->tb_struct.type_num ; k++) {
            res.append(tb->prorecord[i][k]);
            res.append("\t\t");
        }
        res.append("\n");
    }

    return res;
}

int threadinfo::alter_sql(string * sqlseg , int segnum) {
    // alter table tb_name ...
    if (sqlseg[1] == "table") {
        // alter table tb_name add field type [comment '...'] [attribute]
        if (segnum >= 6 && sqlseg[3] == "add") {
            string tb_name = sqlseg[2];
            string field = sqlseg[4];
            string type = sqlseg[5];
            string comment = "";
            string attr = "";
            // alter table tb_name add field type comment '...' [attr]
            if (segnum >= 8 && segnum <= 9 && sqlseg[6] == "comment") {
                // alter table tb_name add field type comment '...'
                if (sqlseg[7][0] == '\'' && sqlseg[7].size() >= 2 && sqlseg[7][sqlseg[7].size() - 1] == '\'') {
                    for (int i = 1 ; i < sqlseg[7].size() - 1 ; i++)
                        comment = comment + sqlseg[7][i];
                    // alter table tb_name add field type comment '...' attr
                    if (segnum == 9) {
                        attr = sqlseg[8];
                    }
                }
                else {
                    return 4;
                }
            }
            // alter table tb_name add field type attr
            else if (segnum == 7) {
                attr = sqlseg[6];
            }
            int res;
            res = add_field(tb_name , field , type , comment , attr);
            if (res == -1)
                return 8;
            else if (res == 2)
                return 11;
            else if (res == 1) 
                return 6;
            else if (res == 3)
                return 12;
            else if (res == 0) {
                string msg = "add field success\n";
                if (send(this->_fd , (char *)msg.data() , msg.size() , 0) == -1)
                    fprintf(logfp , "threadinfo::alter_sql : send error\n");
                return 0;
            }
        }
        // alter table tb_name modify field type
        else if (segnum == 6 && sqlseg[3] == "modify") {
            string tb_name = sqlseg[2];
            string oldfield = sqlseg[4];
            string type = sqlseg[5];
            int res;
            res = modify_field(tb_name , oldfield , type);
            if (res == -1)
                return 8;
            else if (res == 1)
                return 6;
            else if (res == 2)
                return 13;
            else if (res == 3)
                return 12;
            else if (res == 0) {
                string msg = "modify field success\n";
                if (send(this->_fd , (char *)msg.data() , msg.size() , 0) == -1)
                    fprintf(logfp , "threadinfo::alter_sql : send error\n");
                return 0;
            }
        }
        // alter table tb_name change oldfield newfield type [comment '...'] [attr]
        else if (segnum >= 7 && sqlseg[3] == "change") {
            string tb_name = sqlseg[2];
            string oldfield = sqlseg[4];
            string newfield = sqlseg[5];
            string type = sqlseg[6];
            string comment = "";
            string attr = "";

            // alter table tb_name change field type comment '...' [attr]
            if (segnum >= 9 && segnum <= 10 && sqlseg[7] == "comment") {
                // alter table tb_name change field type comment '...'
                if (sqlseg[8][0] == '\'' && sqlseg[8].size() >= 2 && sqlseg[8][sqlseg[8].size() - 1] == '\'') {
                    for (int i = 1 ; i < sqlseg[8].size() - 1 ; i++)
                        comment = comment + sqlseg[8][i];
                    // alter table tb_name change field type comment '...' attr
                    if (segnum == 10) {
                        attr = sqlseg[9];
                    }
                }
                else {
                    return 4;
                }
            }
            // alter table tb_name change field type attr
            else if (segnum == 8) {
                attr = sqlseg[7];
            }
            int res;
            res = change_field(tb_name , oldfield , newfield , type , comment , attr);
            if (res == -1)
                return 8;
            else if (res == 2)
                return 13;
            else if (res == 1) 
                return 6;
            else if (res == 3)
                return 12;
            else if (res == 4)
                return 11;
            else if (res == 0) {
                string msg = "change field success\n";
                if (send(this->_fd , (char *)msg.data() , msg.size() , 0) == -1)
                    fprintf(logfp , "threadinfo::alter_sql : send error\n");
                return 0;
            }
        }
        // alter table tb_name drop field
        else if (segnum == 5 && sqlseg[3] == "drop") {
            string tb_name = sqlseg[2];
            string field = sqlseg[4];
            int res;
            res = drop_field(tb_name , field);
            if (res == -1)
                return 8;
            else if (res == 1)
                return 10;
            else if (res == 2)
                return 13;
            else if (res == 0) {
                string msg = "drop field success\n";
                if (send(this->_fd , (char *)msg.data() , msg.size() , 0) == -1)
                    fprintf(logfp , "threadinfo::alter_sql : send error\n");
                return 0;
            }
        }
        // alter table tb_name rename to newname
        else if (segnum == 6 && sqlseg[3] == "rename" && sqlseg[4] == "to") {
            string tb_name = sqlseg[2];
            string newname = sqlseg[5];
            int res;
            res = rename_table(tb_name , newname);
            if (res == -1)
                return 8;
            else if (res == 1)
                return 6;
            else if (res == 0) {
                string msg = "rename table success\n";
                if (send(this->_fd , (char *)msg.data() , msg.size() , 0) == -1)
                    fprintf(logfp , "threadinfo::alter_sql : send error\n");
                return 0;
            }
        }
    }
    return 4;
}