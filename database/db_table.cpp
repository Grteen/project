#include "db_table.h"

void table::process_record() {
    for (int i = 0 ; i < this->record_num ; i++)
        for (int j = 0 ; j < this->tb_struct.type_num ; j++)
            this->prorecord[i][j] = "";                 // put NULL
    for (int i = 0 ; i < this->record_num ; i++) {
        std::string record = this->record[i];
        int typei = 0;
        for (int j = 0 ; j < record.size() ; j++) {
            if (record[j] != ':')       // the content of a type
                this->prorecord[i][typei] = this->prorecord[i][typei] + record[j];
            else {           // find the RECORDSEP
                typei++;        // go to next type
                // if next char is NULL or next char is ':' too
                if (j + 1 == record.size() || record[j + 1] == ':')
                    this->prorecord[i][typei] = "NULL";
            }
        }
    }
}

void table::check_data_validity() {
    for (int i = 0 ; i < this->record_num ; i++) {
        for (int j = 0 ; j < this->tb_struct.type_num ; j++) {
            if (tb_struct.tb_type[j] == "INT") {
                INT temp;
                if (prorecord[i][j] == "NULL")
                    temp.val = 0;
                else 
                    temp.val = std::stol(this->prorecord[i][j]);
                if (temp.INT_CHECK() == 1)      // invalid
                    this->prorecord[i][j] = "NULL"; 
            }
            if (tb_struct.tb_type[j] == "FLOAT") {
                FLOAT temp;
                if (prorecord[i][j] == "NULL")
                    temp.val = 0;
                else 
                    temp.val = std::stof(this->prorecord[i][j]);
                if (temp.FLOAT_CHECK() == 1)      // invalid
                    this->prorecord[i][j] = "NULL"; 
            }
            if (tb_struct.tb_type[j] == "DOUBLE") {
                DOUBLE temp;
                if (prorecord[i][j] == "NULL")
                    temp.val = 0;
                else 
                    temp.val = std::stod(this->prorecord[i][j]);
                if (temp.DOUBLE_CHECK() == 1)      // invalid
                    this->prorecord[i][j] = "NULL"; 
            }
            if (tb_struct.tb_type[j] == "CHAR") {
                CHAR temp;
                    temp.val = this->prorecord[i][j];
                if (temp.CHAR_CHECK() == 1)      // invalid
                    this->prorecord[i][j] = "NULL"; 
            }
            if (tb_struct.tb_type[j] == "VARCHAR") {
                VARCHAR temp;
                temp.val = this->prorecord[i][j];
                if (temp.VARCHAR_CHECK() == 1)      // invalid
                    this->prorecord[i][j] = "NULL"; 
            }     
            if (tb_struct.tb_type[j] == "YEAR") {
                YEAR temp;
                if (prorecord[i][j] == "NULL")
                    temp.val = 0;
                else 
                    temp.val = std::stol(this->prorecord[i][j]);
                if (temp.YEAR_CHECK() == 1)      // invalid
                    this->prorecord[i][j] = "NULL"; 
            }
            if (tb_struct.tb_type[j] == "TIME") {
                TIME temp;
                std::string str;
                int times = 0;
                if (prorecord[i][j] == "NULL") {
                    temp.hour = 0;
                    temp.minute = 0;
                    temp.second = 0;
                }
                for (int k = 0 ; k < this->prorecord[i][j].size(); k++) {
                    if (prorecord[i][j][k] != ':') {
                        str = str + prorecord[i][j][k];
                    }
                    else {
                        if (times == 0) 
                            temp.hour = std::stol(str);
                        if (times == 1)
                            temp.minute = std::stol(str);
                        if (times == 2)
                            temp.minute = std::stol(str);
                        times++;
                        str = "";
                    }
                }
                if (temp.TIME_CHECK() == 1)     // invalid
                    this->prorecord[i][j] = "NULL";
            }
            if (tb_struct.tb_type[j] == "DATE") {
                DATE temp;
                std::string str;
                int times = 0;
                if (prorecord[i][j] == "NULL") {
                    temp.year = 0;
                    temp.month = 0;
                    temp.day = 0;
                }
                for (int k = 0 ; k < this->prorecord[i][j].size(); k++) {
                    if (prorecord[i][j][k] != '-') {
                        str = str + prorecord[i][j][k];
                    }
                    else {
                        if (times == 0) 
                            temp.year = std::stol(str);
                        if (times == 1)
                            temp.month = std::stol(str);
                        if (times == 2)
                            temp.year = std::stol(str);
                        times++;
                        str = "";
                    }
                }
                if (temp.DATE_CHECK() == 1)     // invalid
                    this->prorecord[i][j] = "NULL";
            }
        }
    }
}

void table::write_record() {
    for (int i = 0 ; i < this->record_num ; i++) {
        this->record[i] = "";           // put NULL
        for (int j = 0 ; j < this->tb_struct.type_num ; j++) {
            this->record[i].append(this->prorecord[i][j]);
            if (j != this->tb_struct.type_num - 1)
                this->record[i].append(RECORDSEP);
        }
    }
}

void table_structure::process_attr() {
    for (int i = 0 ; i < this->type_num ; i++)
        for (int j = 0 ; j < TBTYATTRMAX ; j++)
            this->tb_proattr[i][j] = "";                 // put NULL
    for (int i = 0 ; i < this->type_num ; i++) {
        std::string attr = this->tb_tpattr[i];
        int attri = 0;
        for (int j = 0 ; j < attr.size() ; j++) {
            if (attr[j] != ATTRSEP)       // the content of a attribute line
                this->tb_proattr[i][attri] = this->tb_proattr[i][attri] + attr[j];
            else            // find the RECORDSEP
                attri++;        // go to next type
        }
        this->tb_attrnum[i] = attri + 1;
    }
}

void table_structure::write_attr() {
    for (int i = 0 ; i < this->type_num ; i++) {
        this->tb_tpattr[i] = "";           // put NULL
        for (int j = 0 ; j < this->tb_attrnum[i] ; j++) {
            this->tb_tpattr[i].append(this->tb_proattr[i][j]);
            if (j != this->tb_attrnum[i] - 1)
                this->tb_tpattr[i].append(",");
        }
    }
}