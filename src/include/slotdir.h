#ifndef PETERDB_SLOTDIR_H
#define PETERDB_SLOTDIR_H

#include<vector>
#include "pfm.h"

namespace PeterDB {
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
}
#endif //PETERDB_SLOTDIR_H
