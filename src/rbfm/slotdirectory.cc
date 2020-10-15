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
        // Add to slot dir
        Slot* newDir = (Slot *) malloc((this->recordCount + 1) * sizeof(Slot));
        memcpy(newDir, this->slots, this->recordCount * sizeof(Slot));
        free(this->slots);
        this->slots = newDir;
        *(this->slots + this->recordCount) = {offset, recordLength};

        // reduce free space by one slot size
        this->freeSpace = this->freeSpace - sizeof(Slot);
        this->recordCount = this->recordCount + 1;
        return 0;
    }

    unsigned short SlotDirectory::getRecordLength(unsigned short slotNum) const {
        return slots[slotNum].length;
    }

    unsigned short SlotDirectory::getRecordOffset(unsigned short slotNum) const {
        return slots[slotNum].offset;
    }
}