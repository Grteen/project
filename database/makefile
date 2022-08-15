OBJS = database_server.o cli_control.o sev_select.o common.o thread_pool.o
main: ${OBJS} database_client.o
	g++ -o client database_client.o common.o
	g++ -g -o main ${OBJS}
clean:
	rm -f main ${OBJS}
	rm -f client database_client.o
test: DDL.o common.o db_type.o db_table.o 
	g++ -o a.out DDL.o common.o db_type.o db_table.o