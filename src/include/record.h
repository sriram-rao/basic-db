#ifndef PETERDB_RECORD_H
#define PETERDB_RECORD_H

#include "rbfm.h";

namespace PeterDB {
    class Record {
    protected:
        int countOfAttributes;
        int *fieldOffset;
        unsigned char* values;

    public:
        RID* rid;
        Record();

        Record(int countOfAttributes, int *fieldOffset, unsigned char* values);

        operator const char *();

        virtual ~Record();
    };
}

#endif //PETERDB_RECORD_H
