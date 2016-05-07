// This file includes shared constants and other values.
#ifndef COMMON_H__
#define COMMON_H__

#define SOCK_PATH "cs165_unix_socket"

/**
 * EXTRA
 * DataType
 * Flag to mark what type of data is held in the struct.
 * You can support additional types by including this enum and using void*
 * in place of int* in db_operator simliar to the way IndexType supports
 * additional types.
 **/
typedef enum DataType {
     INT,
     LONG,
     CHAR,
     LONG_DOUBLE,
     // Others??
} DataType;

#endif  // COMMON_H__
