#ifndef PETERDB_PAGE_H
#define PETERDB_SLOTDIRECTORY_H

#include<vector>
#include "pfm.h"
#include "rbfm.h"

using namespace std;

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

    class SlotDirectory {
    protected:
        short freeSpace;
        short recordCount;
        short** slots;
    public:
        SlotDirectory();
        SlotDirectory(short freeSpace, short recordCount, short** slots);
        virtual ~SlotDirectory();

        RC addSlot(short offset, short recordLength);
    };

    class Page {
    public:
        SlotDirectory directory;
        vector<Record> records;
        Page();
        Page(SlotDirectory directory, vector<Record> records);
        ~Page();
    };
}
#endif //PETERDB_PAGE_H
