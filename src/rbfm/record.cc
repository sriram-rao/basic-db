#include "src/include/rbfm.h"

namespace PeterDB {
    Record::Record() = default;

    Record::Record(RID id, short countOfAttributes, unsigned short* offsets, unsigned char* values) {
        this->rid = id;
        this->attributeCount = countOfAttributes;
        this->fieldOffsets = offsets;
        this->values = values;
    }

    char * Record::to_bytes() {}

    Record::~Record() = default;
}