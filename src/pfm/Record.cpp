#include "src/include/Record.h"

Record::Record() { }

Record::Record(int pageNo, int noOfAttributes, char* slotDirOffset, char* valueOffsets, char* values) {
    this->pageNo = pageNo;
    this->noOfAttributes = noOfAttributes;
    this->slotDirOffset = slotDirOffset;
    this->valueOffsets = valueOffsets;
    this->values = values;
}

Record::~Record() = default;