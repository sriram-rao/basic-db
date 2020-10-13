#include "src/include/slotdir.h"

namespace PeterDB {
    SlotDirectory::SlotDirectory() {}

    SlotDirectory::SlotDirectory(short freeSpace, short recordCount, short** slots) {
        this->freeSpace = freeSpace;
        this->recordCount = recordCount;
        this->slots = slots;
    }

    SlotDirectory::~SlotDirectory() = default;

    RC SlotDirectory::addSlot(short offset, short recordLength) {
        // TODO :: Add to slot dir
        return -1;
    }
}