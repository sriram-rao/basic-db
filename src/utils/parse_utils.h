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
        static string parse(AttrType type, char *data) {
            switch (type) {
                case TypeInt:
                    return parseInt(data);
                case TypeReal:
                    return parseReal(data);
                case TypeVarChar:
                    return string (data);
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
    };
}
#endif //PETERDB_PARSE_UTILS_H
