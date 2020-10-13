#include "src/include/Slot.h"

Slot::Slot() { }

Slot ::Slot(char *recordValueOffset, int sizeOfRecord) {
    this->recordValueOffset = recordValueOffset;
    this->sizeOfRecord = sizeOfRecord;
}

Slot::~Slot() = default;