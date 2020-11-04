#include <utility>


#include "src/include/rbfm.h"

namespace PeterDB {
    SlotDirectory::SlotDirectory() {}

    SlotDirectory::SlotDirectory(short freeSpace, short recordCount) {
        this->freeSpace = freeSpace;
        this->recordCount = recordCount;
    }

    SlotDirectory & SlotDirectory::operator=(const SlotDirectory &other) = default;

    SlotDirectory::~SlotDirectory() = default;

    RC SlotDirectory::addSlot(unsigned short slotNum, short offset, short recordLength) {
        this->slots.push_back({offset, recordLength});
        this->freeSpace = this->freeSpace - sizeof(Slot);
        this->recordCount++;
        return 0;
    }

    RC SlotDirectory::setSlot(unsigned short slotNum, short recordLength) {
        short offset = 0;
        for (int i = 0; i < slotNum; ++i)
            offset += slots[i].length;
        slots[slotNum].offset = offset;
        slots[slotNum].length = recordLength;
        return 0;
    }

    RC SlotDirectory::updateSlot(unsigned short slotNum, short recordLength) {
        slots[slotNum].length = recordLength;
        return 0;
    }

    short SlotDirectory::getRecordLength(short slotNum) const {
        return slots[slotNum].length == -1 ? (short)0 : slots[slotNum].length;
    }

    short SlotDirectory::getRecordOffset(short slotNum) const {
        return slots[slotNum].offset;
    }
}