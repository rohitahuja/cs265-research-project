#define _GNU_SOURCE
#define _XOPEN_SOURCE
/**
 * client.c
 *  CS165 Fall 2015
 *
 * This file provides a basic unix socket implementation for a client
 * used in an interactive client-server database.
 * The client receives input from stdin and sends it to the server.
 * No pre-processing is done on the client-side.
 *
 * For more information on unix sockets, refer to:
 * http://beej.us/guide/bgipc/output/html/multipage/unixsock.html
 **/
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>

#include "common.h"
#include "message.h"
#include "utils.h"

#define DEFAULT_STDIN_BUFFER_SIZE 1024

/**
 * connect_client()
 *
 * This sets up the connection on the client side using unix sockets.
 * Returns a valid client socket fd on success, else -1 on failure.
 *
 **/
int connect_client() {
    int client_socket;
    size_t len;
    struct sockaddr_un remote;

    log_info("Attempting to connect...\n");

    if ((client_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        log_err("L%d: Failed to create socket.\n", __LINE__);
        return -1;
    }

    remote.sun_family = AF_UNIX;
    strncpy(remote.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
    len = strlen(remote.sun_path) + sizeof(remote.sun_family) + 1;
    if (connect(client_socket, (struct sockaddr *)&remote, len) == -1) {
        perror("client connect failed: ");
        return -1;
    }

    log_info("Client connected at socket: %d.\n", client_socket);
    return client_socket;
}

int main(void)
{
    int client_socket = connect_client();
    if (client_socket < 0) {
        exit(1);
    }

    message send_message;
    message recv_message;

    // Always output an interactive marker at the start of each command if the
    // input is from stdin. Do not output if piped in from file or from other fd
    char* prefix = "";
    if (isatty(fileno(stdin))) {
        prefix = "db_client > ";
    }

    char *output_str = NULL;
    int len = 0;

    // Continuously loop and wait for input. At each iteration:
    // 1. output interactive marker
    // 2. read from stdin until eof.
    char read_buffer[DEFAULT_STDIN_BUFFER_SIZE];
    send_message.payload = read_buffer;

    while (printf("%s", prefix), output_str = fgets(read_buffer,
           DEFAULT_STDIN_BUFFER_SIZE, stdin), !feof(stdin)) {
        if (output_str == NULL) {
            log_err("fgets failed.\n");
            break;
        }

        // Only process input that is greater than 1 character.
        // Ignore things such as new lines.
        // Otherwise, convert to message and send the message and the
        // payload directly to the server.
        send_message.length = strlen(read_buffer);
        if (send_message.length > 1) {
            if (strncmp("load", read_buffer, 4) == 0) {
                // Create a working copy, +1 for '\0'
                char* str_cpy = malloc(strlen(read_buffer) + 1);
                strncpy(str_cpy, read_buffer, strlen(read_buffer) + 1);

                // This gives us everything inside the parens
                strtok(str_cpy, "\"");
                char* path = strtok(NULL, "\"");
                
                // Open file for bulk load
                FILE *fd = fopen(path, "r");

                if (fd == NULL) {
                    log_err("Failed to open file\n");
                    continue;
                }

                char * line = NULL;
                size_t line_len = 0;
                ssize_t read = getline(&line, &line_len, fd);

                send_message.status = LOAD_REQUEST;
                send_message.length = line_len;
                
                // Send a request to load data
                if (send(client_socket, &(send_message), sizeof(message), 0) == -1) {
                    log_err("Failed to send message header.\n");
                    exit(1);
                }

                send_message.payload = line;

                // Send the first line of the file, which tells the column metadata
                if (send(client_socket, send_message.payload, send_message.length, 0) == -1) {
                    log_err("Failed to send message header.\n");
                    exit(1);
                }

                // Send the data to server
                while((read = getline(&line, &line_len, fd)) != -1) {
                    send_message.length = line_len;
                    send_message.status = OK_WAIT_FOR_RESPONSE;

                    // Send length of next line
                    if (send(client_socket, &(send_message), sizeof(message), 0) == -1) {
                        log_err("Failed to send message header.\n");
                        exit(1);
                    }

                    send_message.payload = line;
                    if (send(client_socket, send_message.payload, send_message.length, 0) == -1) {
                        log_err("Failed to send query payload.\n");
                        exit(1);
                    }
                }

                fclose(fd);

                send_message.status = LOAD_DONE;
                if (send(client_socket, &(send_message), sizeof(message), 0) == -1) {
                    log_err("Failed to send query payload.\n");
                    exit(1);
                }

                send_message.payload = read_buffer;

            } else {
                // Send the message_header, which tells server payload size
                if (send(client_socket, &(send_message), sizeof(message), 0) == -1) {
                    log_err("Failed to send message header.");
                    exit(1);
                }

                // Send the payload (query) to server
                if (send(client_socket, send_message.payload, send_message.length, 0) == -1) {
                    log_err("Failed to send query payload.");
                    exit(1);
                }
            }

            // Always wait for server response (even if it is just an OK message)
            if ((len = recv(client_socket, &(recv_message), sizeof(message), 0)) > 0) {
                if (recv_message.status == OK_WAIT_FOR_RESPONSE &&
                    (int) recv_message.length > 0) {
                    // Calculate number of bytes in response package
                    size_t num_bytes = recv_message.length;
                    char payload[num_bytes + 1];

                    size_t num_rows = recv_message.num_rows;
                    size_t num_cols = recv_message.num_cols;

                    size_t received = 0;
                    char* payload_ptr = payload;
                    
                    // Receive the payload and print it out
                    while(received < num_bytes && (len = recv(client_socket, payload_ptr, num_bytes, 0)) > 0) {
                        payload_ptr += len;
                        received += len;
                    }
                    payload[num_bytes] = '\0';

                    if (recv_message.type == INT) {
                        int* values = (int*)payload;
                        for(size_t i = 0; i < num_rows; i++) {
                            for(size_t j = 0; j < num_cols; j++) {
                                printf("%d", values[i + j*num_rows]);
                                if (j != num_cols -1) {
                                    printf(",");
                                }
                            }
                            printf("\n");
                        }
                    } else if (recv_message.type == LONG) {
                        long* values = (long*)payload;
                        for(size_t i = 0; i < num_rows; i++) {
                            for(size_t j = 0; j < num_cols; j++) {
                                printf("%ld", values[i + j*num_rows]);
                                if (j != num_cols -1) {
                                    printf(",");
                                }
                            }
                            printf("\n");
                        }
                    } else if (recv_message.type == LONG_DOUBLE) {
                        long double* values = (long double*)payload;
                        for(size_t i = 0; i < num_rows; i++) {
                            for(size_t j = 0; j < num_cols; j++) {
                                printf("%.12Lf", values[i + j*num_rows]);
                                if (j != num_cols -1) {
                                    printf(",");
                                }
                            }
                            printf("\n");
                        }
                    } else if (recv_message.type == CHAR) {
                        log_info("%s", payload);
                    }
                } else if (recv_message.status == SHUTDOWN_CLIENT) {
                    break;
                }
            }
            else {
                if (len < 0) {
                    log_err("Failed to receive message.");
                }
                else {
                    log_info("Server closed connection\n");
                }
                exit(1);
            }
        }
    }
    close(client_socket);
    return 0;
}