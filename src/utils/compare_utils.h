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
        static bool check(AttrType type, CompOp op, const void *lhs, const void *rhs) {
            switch (op) {
                case EQ_OP:
                    return checkEqual(type, lhs, rhs);
                case LT_OP:
                    return checkLessThan(type, lhs, rhs);
                case LE_OP:
                    return checkLessThan(type, lhs, rhs) || checkEqual(type, lhs, rhs);
                case GT_OP:
                    return checkGreaterThan(type, lhs, rhs);
                case GE_OP:
                    return checkGreaterThan(type, lhs, rhs) || checkEqual(type, lhs, rhs);
                case NE_OP:
                    return !checkEqual(type, lhs, rhs);
                case NO_OP:
                    return true;
            }
        }

        static bool checkEqual(AttrType type, const void *lhs, const void *rhs) {
            if (TypeVarChar == type) {
                string s1 = ParseUtils::parseVarchar(const_cast<void *>(lhs));
                string s2 = ParseUtils::parseVarchar(const_cast<void *>(rhs));
                return s1 == s2;
            }

            if (TypeInt == type) {
                int n1, n2;
                std::memcpy(&n1, lhs, sizeof(int));
                std::memcpy(&n2, rhs, sizeof(int));
                return n1 == n2;
            }

            float f1, f2;
            std::memcpy(&f1, lhs, sizeof(float));
            std::memcpy(&f2, rhs, sizeof(float));
            return f1 == f2;
        }

        static bool checkLessThan(AttrType type, const void *lhs, const void *rhs) {
            if (TypeVarChar == type) {
                string s1 = ParseUtils::parseVarchar(const_cast<void *>(lhs));
                string s2 = ParseUtils::parseVarchar(const_cast<void *>(rhs));
                return s1 < s2;
            }

            if (TypeInt == type) {
                int n1, n2;
                std::memcpy(&n1, lhs, sizeof(int));
                std::memcpy(&n2, rhs, sizeof(int));
                return n1 < n2;
            }

            float f1, f2;
            std::memcpy(&f1, lhs, sizeof(float));
            std::memcpy(&f2, rhs, sizeof(float));
            return f1 < f2;
        }

        static bool checkGreaterThan(AttrType type, const void *lhs, const void *rhs) {
            if (TypeVarChar == type) {
                string s1 = ParseUtils::parseVarchar(const_cast<void *>(lhs));
                string s2 = ParseUtils::parseVarchar(const_cast<void *>(rhs));
                return s1 > s2;
            }

            if (TypeInt == type) {
                int n1, n2;
                std::memcpy(&n1, lhs, sizeof(int));
                std::memcpy(&n2, rhs, sizeof(int));
                return n1 > n2;
            }

            float f1, f2;
            std::memcpy(&f1, lhs, sizeof(float));
            std::memcpy(&f2, rhs, sizeof(float));
            return f1 > f2;
        }
    };
}
#endif //PETERDB_COMPARE_UTILS_H
