//
// Created by sriram on 11/16/20.
//

#ifndef PETERDB_COMPARE_UTILS_H
#define PETERDB_COMPARE_UTILS_H
#include <cstring>
#include "parse_utils.h"
#include <src/include/rbfm.h>

using namespace std;

namespace PeterDB {
    class CompareUtils {
    public:
        static bool checkEqual(AttrType type, const void *value1, const void *value2) {
            if (TypeVarChar == type) {
                string s1 = ParseUtils::parseVarchar(const_cast<void *>(value1));
                string s2 = ParseUtils::parseVarchar(const_cast<void *>(value2));
                return s1 == s2;
            }

            if (TypeInt == type) {
                int n1, n2;
                std::memcpy(&n1, value1, sizeof(int));
                std::memcpy(&n2, value2, sizeof(int));
                return n1 == n2;
            }

            float f1, f2;
            std::memcpy(&f1, value1, sizeof(float));
            std::memcpy(&f2, value2, sizeof(float));
            return f1 == f2;
        }

        static bool checkLessThan(AttrType type, const void *value1, const void *value2) {
            if (TypeVarChar == type) {
                string s1 = ParseUtils::parseVarchar(const_cast<void *>(value1));
                string s2 = ParseUtils::parseVarchar(const_cast<void *>(value2));
                return s1 < s2;
            }

            if (TypeInt == type) {
                int n1, n2;
                std::memcpy(&n1, value1, sizeof(int));
                std::memcpy(&n2, value2, sizeof(int));
                return n1 < n2;
            }

            float f1, f2;
            std::memcpy(&f1, value1, sizeof(float));
            std::memcpy(&f2, value2, sizeof(float));
            return f1 < f2;
        }

        static bool checkGreaterThan(AttrType type, const void *value1, const void *value2) {
            if (TypeVarChar == type) {
                string s1 = ParseUtils::parseVarchar(const_cast<void *>(value1));
                string s2 = ParseUtils::parseVarchar(const_cast<void *>(value2));
                return s1 > s2;
            }

            if (TypeInt == type) {
                int n1, n2;
                std::memcpy(&n1, value1, sizeof(int));
                std::memcpy(&n2, value2, sizeof(int));
                return n1 > n2;
            }

            float f1, f2;
            std::memcpy(&f1, value1, sizeof(float));
            std::memcpy(&f2, value2, sizeof(float));
            return f1 > f2;
        }
    };
}
#endif //PETERDB_COMPARE_UTILS_H
