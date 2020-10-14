#include "src/include/rbfm.h"

namespace PeterDB {
    SlotDirectory::SlotDirectory() {}

    SlotDirectory::SlotDirectory(short freeSpace, short recordCount, Slot* slots) {
        this->freeSpace = freeSpace;
        this->recordCount = recordCount;
        this->slots = slots;
    }

    SlotDirectory::~SlotDirectory() = default;

    RC SlotDirectory::addSlot(unsigned short offset, unsigned short recordLength) {
        // TODO :: Add to slot dir
        return -1;
    }

    unsigned short SlotDirectory::getRecordLength(unsigned short slotNum) const {
        return slots[slotNum].length;
    }

    unsigned short SlotDirectory::getRecordOffset(unsigned short slotNum) const {
        return slots[slotNum].offset;
    }
}