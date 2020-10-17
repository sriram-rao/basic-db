#include "src/include/rbfm.h"

namespace PeterDB {
    SlotDirectory::SlotDirectory() {}

    SlotDirectory::SlotDirectory(short freeSpace, short recordCount, vector<Slot> slots) {
        this->freeSpace = freeSpace;
        this->recordCount = recordCount;
        this->slots = slots;
    }

    SlotDirectory::~SlotDirectory() = default;

    RC SlotDirectory::addSlot(unsigned short offset, unsigned short recordLength) {
        this->slots.push_back({ offset, recordLength });
        this->freeSpace = this->freeSpace  - sizeof(Slot);
        return 0;
    }

    unsigned short SlotDirectory::getRecordLength(unsigned short slotNum) const {
        return slots[slotNum].length;
    }

    unsigned short SlotDirectory::getRecordOffset(unsigned short slotNum) const {
        return slots[slotNum].offset;
    }
}