#include "src/include/rbfm.h"

namespace PeterDB {
    SlotDirectory::SlotDirectory() {}

    SlotDirectory::SlotDirectory(short freeSpace, short recordCount, vector<Slot> slots) {
        this->freeSpace = freeSpace;
        this->recordCount = recordCount;
        this->slots = slots;
    }

    SlotDirectory::~SlotDirectory() = default;

    RC SlotDirectory::addSlot(short offset, short recordLength) {
        this->slots.push_back({ offset, recordLength });
        this->freeSpace = this->freeSpace  - sizeof(Slot);
        return 0;
    }

    short SlotDirectory::getRecordLength(short slotNum) const {
        return slots[slotNum].length;
    }

    short SlotDirectory::getRecordOffset(short slotNum) const {
        return slots[slotNum].offset;
    }
}