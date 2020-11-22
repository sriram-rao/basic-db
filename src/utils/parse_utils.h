//
// Created by sriram on 11/14/20.
//

#ifndef PETERDB_PARSE_UTILS_H
#define PETERDB_PARSE_UTILS_H

#include <cstring>
#include <string>
#include <src/include/rbfm.h>

using namespace std;

namespace PeterDB {
    class ParseUtils {
    public:
        static string parse(AttrType type, char *data, int length) {
            switch (type) {
                case TypeInt:
                    return parseInt(data);
                case TypeReal:
                    return parseReal(data);
                case TypeVarChar:
                    return string (data, length);
            }
        }

        static string parseInt(char *data) {
            int value;
            std::memcpy(&value, data, sizeof(int));
            return to_string(value);
        }

        static string parseReal(char *data) {
            float value;
            std::memcpy(&value, data, sizeof(float));
            return to_string(value);
        }

        static string parseVarchar(void* data){
            int fieldLength;
            std::memcpy(&fieldLength, data, 4);
            char field [fieldLength + 1];
            std::memcpy(field, (char *)data + 4, fieldLength);
            field[fieldLength] = '\0';
            return string (field);
        }
    };
}
#endif //PETERDB_PARSE_UTILS_H
