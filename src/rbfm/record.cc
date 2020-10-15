#include "src/include/rbfm.h"

namespace PeterDB {
    Record::Record() = default;

    Record::Record(RID id, short countOfAttributes, unsigned short* offsets, unsigned char* values) {
        this->rid = id;
        this->attributeCount = countOfAttributes;
        this->fieldOffsets = offsets;
        this->values = values;
    }

    Record Record::fromBytes(unsigned char* bytes) {
        short attributeCount;
        memcpy(&attributeCount, bytes, sizeof(short));
        unsigned short offsets[attributeCount];
        memcpy(offsets, bytes + sizeof(short), sizeof(offsets));
        unsigned long dataSize = offsets[attributeCount - 1] - sizeof(short) - sizeof(offsets);
        unsigned char* data = (unsigned char*) malloc(dataSize);
        memcpy(data, bytes + sizeof(short) + sizeof(offsets), dataSize);
        return Record({0, 0}, attributeCount, offsets, data);
    }

    unsigned char* Record::toBytes(u_short recordLength) {
        u_short dataSize = recordLength - sizeof(short) - sizeof(short) * attributeCount;
        unsigned char* byteArray = (unsigned char*) malloc(sizeof(fieldOffsets) + sizeof(short) + sizeof(values));
        memcpy(byteArray, &attributeCount, sizeof(short));
        memcpy(byteArray + sizeof(short), &fieldOffsets, sizeof(fieldOffsets));
        memcpy(byteArray + sizeof(short) + sizeof(fieldOffsets), values, dataSize);
        return byteArray;
    }

    Record::~Record() = default;
}