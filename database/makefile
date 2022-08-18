OBJS = BPT.o cli_control.o common.o database_server.o db_table.o db_type.o DDL.o DML.o DQL.o index.o sev_select.o thread_pool.o
main: ${OBJS} database_client.o
	g++ -o client database_client.o common.o
	g++ -g -o main ${OBJS}
clean:
	rm -f main ${OBJS}
	rm -f client database_client.o