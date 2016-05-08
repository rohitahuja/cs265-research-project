/** server.c
 * CS165 Fall 2015
 *
 * This file provides a basic unix socket implementation for a server
 * used in an interactive client-server database.
 * The server should allow for multiple concurrent connections from clients.
 * Each client should be able to send messages containing queries to the
 * server.  When the server receives a message, it must:
 * 1. Respond with a status based on the query (OK, UNKNOWN_QUERY, etc.)
 * 2. Process any appropriate queries, if applicable.
 * 3. Return the query response to the client.
 *
 * For more information on unix sockets, refer to:
 * http://beej.us/guide/bgipc/output/html/multipage/unixsock.html
 **/
#define _GNU_SOURCE

#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/socket.h>
#include <unistd.h>
#include <string.h>
#include <limits.h>

#include "common.h"
#include "cs165_api.h"
#include "message.h"
#include "parser.h"
#include "utils.h"
#include "helpers.h"

#define DEFAULT_QUERY_BUFFER_SIZE 1024
#define change 10

// Here, we allow for a global of DSL COMMANDS to be shared in the program
dsl** dsl_commands;

// Instantiating global global_db to map hold record of db
db* global_db;

// Variable pool for clients
catalog** catalogs;

/**
 * parse_command takes as input the send_message from the client and then
 * parses it into the appropriate query. Stores into send_message the
 * status to send back.
 * Returns a db_operator.
 **/
db_operator* parse_command(message* recv_message, message* send_message, status* parse_status) {
    send_message->status = OK_WAIT_FOR_RESPONSE;
    // check load flags here
    db_operator *dbo = init_dbo();
    // Here you parse the message and fill in the proper db_operator fields for
    // now we just log the payload
    cs165_log(stdout, recv_message->payload);

    // Here, we give you a default parser, you are welcome to replace it with anything you want
    *parse_status = parse_command_string(recv_message->payload, dsl_commands, dbo);

    return dbo;
}

/** execute_db_operator takes as input the db_operator and executes the query.
 * It should return the result (currently as a char*, although I'm not clear
 * on what the return type should be, maybe a result struct, and then have
 * a serialization into a string message).
 **/
char* execute_db_operator(db_operator* query) {
    status s;

    if (query->type == INSERT) {
        table* tbl1 = query->tables[0];
        for(size_t i = 0; i < tbl1->col_count; i++) {
            s = col_insert(query->columns[i], query->value1[i]);
            if (s.code != OK) {
                return s.error_message;
            }
        }
        return "Rows successfully inserted.";
    } else if (query->type == SELECT) {
        result* r = malloc(sizeof(struct result));
        if (query->columns) {
            s = select_data(query, &r);
        } else if (query->result1 && query->result2) {
            s = vec_scan(query, &r);
        } else {
            return "Cannot perform select\n";
        }

        if (s.code != OK) {
            return s.error_message;
        }
        int idx = catalogs[0]->var_count;
        catalogs[0]->names[idx] = query->name1;
        catalogs[0]->results[idx] = r;
        catalogs[0]->var_count++;
    } else if (query->type == PROJECT) {
        result* r = malloc(sizeof(struct result));
        status s = fetch(*(query->columns), query->result1->payload, query->result1->num_tuples, &r);
        if (s.code != OK) {
            return s.error_message;
        }
        int idx = catalogs[0]->var_count;
        catalogs[0]->names[idx] = query->name1;
        catalogs[0]->results[idx] = r;
        catalogs[0]->var_count++;
    } else if (query->type == ADD) {
        result* r = malloc(sizeof(struct result));
        s = add_col((int*)query->result1->payload, (int*)query->result2->payload, query->result1->num_tuples, &r);
        if (s.code != OK) {
            return s.error_message;
        }
        int idx = catalogs[0]->var_count;
        catalogs[0]->names[idx] = query->name1;
        catalogs[0]->results[idx] = r;
        // for (size_t i = 0; i < r->num_tuples; ++i)
        // {
        //     printf("%ld\n", ((long*)r->payload)[i]);
        // }
        catalogs[0]->var_count++;
    } else if (query->type == SUB) {
        result* r = malloc(sizeof(struct result));
        s = sub_col((int*)query->result1->payload, (int*)query->result2->payload, query->result1->num_tuples, &r);
        if (s.code != OK) {
            return s.error_message;
        }
        int idx = catalogs[0]->var_count;
        catalogs[0]->names[idx] = query->name1;
        catalogs[0]->results[idx] = r;
        catalogs[0]->var_count++;
    } else if (query->type == AGGREGATE) {
        result* r = malloc(sizeof(struct result));
        if (query->agg == MIN) {
            s = min_col(query->result1, &r);
        } else if (query->agg == MAX) {
            s = max_col(query->result1, &r);
        } else if (query->agg == AVG) {
            s = avg_col(query->result1, &r);
        } else if (query->agg == CNT) {
            s = count_col(query->result1->num_tuples, &r);
        } else {
            return "Failed aggregation";
        }

        if (s.code != OK) {
            return s.error_message;
        }

        int idx = catalogs[0]->var_count;
        catalogs[0]->names[idx] = query->name1;
        catalogs[0]->results[idx] = r;
        catalogs[0]->var_count++;
    }

    return "Success";
}

/**
 * handle_client(client_socket)
 * This is the execution routine after a client has connected.
 * It will continually listen for messages from the client and execute queries.
 **/
void handle_client(int client_socket) {
    int done = 0;
    int length = 0;

    log_info("Connected to socket: %d.\n", client_socket);

    // Create two messages, one from which to read and one from which to receive
    message send_message;
    message recv_message;

    // Continually receive messages from client and execute queries.
    // 1. Parse the command
    // 2. Handle request if appropriate
    // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
    // 4. Send response of request.
    do {
        length = recv(client_socket, &recv_message, sizeof(message), 0);
        if (length < 0) {
            log_err("Client connection closed!\n");
            exit(1);
        } else if (length == 0) {
            done = 1;
        }

        if (recv_message.status == LOAD_REQUEST) {
            char recv_buffer[recv_message.length];
            length = recv(client_socket, recv_buffer, recv_message.length,0);
            recv_message.payload = recv_buffer;
            recv_message.payload[recv_message.length] = '\0';

            char* col_name = strtok(recv_buffer, ",");
            int tbl_idx = find_table_from_col_name(col_name);
            if (tbl_idx == -1) {
                log_err("Cannot find table.\n");
                exit(1);
            }
            table* tbl = global_db->tables[tbl_idx];

            while((length = recv(client_socket, &recv_message, sizeof(message), 0)) > 0) {
                if (recv_message.status == OK_WAIT_FOR_RESPONSE &&
                        (int) recv_message.length > 0) {
                    // Calculate number of bytes in response package
                    size_t num_bytes = recv_message.length;
                    char payload[num_bytes + 1];

                    if ((length = recv(client_socket, payload, num_bytes, 0)) > 0) {
                        db_operator* dbo = init_dbo();
                        status s = relational_insert(tbl_idx, payload, dbo);
                        if (s.code != OK) {
                            log_err(s.error_message);
                            exit(1);
                        }
                        char* result = execute_db_operator(dbo);
                        log_info("%s\n", result);
                    }
                } else if (recv_message.status == LOAD_DONE) {
                    status s = process_indexes(tbl);
                    char* result = "Bulk load done";
                    if (s.code != OK) {
                        result = s.error_message;
                    }

                    send_message.status = OK_WAIT_FOR_RESPONSE;
                    send_message.type = CHAR;
                    send_message.length = strlen(result);

                    // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
                    if (send(client_socket, &(send_message), sizeof(message), 0) == -1) {
                        log_err("Failed to send message.");
                        exit(1);
                    }

                    // 4. Send response of request
                    if (send(client_socket, result, send_message.length, 0) == -1) {
                        log_err("Failed to send message.");
                        exit(1);
                    }

                    break;
                }
            }
            continue;
        }

        if (!done) {
            char recv_buffer[recv_message.length];
            length = recv(client_socket, recv_buffer, recv_message.length,0);
            recv_message.payload = recv_buffer;
            recv_message.payload[recv_message.length] = '\0';

            // 1. Parse command
            status parse_status;
            db_operator* query = parse_command(&recv_message, &send_message, &parse_status);

            // 2. Handle request
            char* result = NULL;
            if (parse_status.code != OK) {
                // Something went wrong
                result = parse_status.error_message;
                send_message.type = CHAR;
                send_message.length = strlen(result);
                
            } else if (query->type == TUPLE) {
                send_message.type = query->tups->type;
                send_message.num_rows = query->tups->num_rows;
                send_message.num_cols = query->tups->num_cols;
                send_message.length = (int) (send_message.num_rows * send_message.num_cols);

                if (send_message.type == INT) {
                    send_message.length *= sizeof(int);
                } else if (send_message.type == LONG) {
                    send_message.length *= sizeof(long);
                } else if (send_message.type == LONG_DOUBLE) {
                    send_message.length *= sizeof(long double);
                } else {
                    send_message.length *= sizeof(char);
                }

                // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
                if (send(client_socket, &(send_message), sizeof(message), 0) == -1) {
                    log_err("Failed to send message.");
                    exit(1);
                }

                // 4. Send response of request
                for(size_t i = 0; i < send_message.num_cols; i++) {
                    char* result = (char*)query->tups->payloads[i];
                    int length = send_message.length/(int)send_message.num_cols;
                    if (send(client_socket, result, length, 0) == -1) {
                        log_err("Failed to send message.");
                        exit(1);
                    }
                }

                continue;

            } else if (query->type == SHUTDOWN) {
                status s = persist_data();
                if (s.code != OK) {
                    log_err("Error persisting data\n");
                }

                // free what you can
                // s = free_catalogs();
                if (s.code != OK) {
                    log_err("Error shutting down\n");
                }

                send_message.status = SHUTDOWN_CLIENT;
                if (send(client_socket, &(send_message), sizeof(message), 0) == -1) {
                    log_err("Failed to send message.");
                    close(client_socket);
                }

                return;
            } else {
                result = execute_db_operator(query);
                send_message.type = CHAR;
                send_message.length = strlen(result);
            }

            // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
            if (send(client_socket, &(send_message), sizeof(message), 0) == -1) {
                log_err("Failed to send message.");
                exit(1);
            }

            // 4. Send response of request
            if (send(client_socket, result, send_message.length, 0) == -1) {
                log_err("Failed to send message.");
                exit(1);
            }
        }
    } while (!done);

    log_info("Connection closed at socket %d!\n", client_socket);
    close(client_socket);
}

/**
 * setup_server()
 *
 * This sets up the connection on the server side using unix sockets.
 * Returns a valid server socket fd on success, else -1 on failure.
 **/
int setup_server() {
    int server_socket;
    size_t len;
    struct sockaddr_un local;

    log_info("Attempting to setup server...\n");

    if ((server_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        log_err("L%d: Failed to create socket.\n", __LINE__);
        return -1;
    }

    local.sun_family = AF_UNIX;
    strncpy(local.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
    unlink(local.sun_path);

    len = strlen(local.sun_path) + sizeof(local.sun_family) + 1;
    if (bind(server_socket, (struct sockaddr *)&local, len) == -1) {
        log_err("L%d: Socket failed to bind.\n", __LINE__);
        return -1;
    }

    if (listen(server_socket, 5) == -1) {
        log_err("L%d: Failed to listen on socket.\n", __LINE__);
        return -1;
    }

    return server_socket;
}

// Currently this main will setup the socket and accept a single client.
// After handling the client, it will exit.
// You will need to extend this to handle multiple concurrent clients
// and remain running until it receives a shut-down command.
int main()
{
    int server_socket = setup_server();
    if (server_socket < 0) {
        exit(1);
    }

    status s = grab_persisted_data();
    if (s.code != OK) {
        log_err(s.error_message);
        return 0;
    }

    // Populate the global dsl commands and catalogs
    dsl_commands = dsl_commands_init();
    catalogs = init_catalogs();

    log_info("Waiting for a connection %d ...\n", server_socket);

    struct sockaddr_un remote;
    socklen_t t = sizeof(remote);
    int client_socket = 0;

    if ((client_socket = accept(server_socket, (struct sockaddr *)&remote, &t)) == -1) {
        log_err("L%d: Failed to accept a new connection.\n", __LINE__);
        exit(1);
    }
    handle_client(client_socket);

    return 0;
}

