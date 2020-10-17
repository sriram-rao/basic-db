#include "src/include/rbfm.h"
#include <cstring>

namespace PeterDB {
    Record::Record() = default;

    Record::Record(RID id, short countOfAttributes, short* offsets, unsigned char* values) {
        this->rid = id;
        this->attributeCount = countOfAttributes;
        this->fieldOffsets = offsets;
        this->values = values;
    }

    Record Record::fromBytes(unsigned char* bytes) {
        short attributeCount;
        memcpy(&attributeCount, bytes, sizeof(short));
        short offsets[attributeCount];
        memcpy(offsets, bytes + sizeof(short), sizeof(offsets));
        unsigned long dataSize;
        for (int i = attributeCount - 1; i >= 0; i--) {
            if (offsets[i] == -1) continue;
            dataSize = offsets[i] - sizeof(short) - sizeof(offsets);
            break;
        }
        unsigned char* data = (unsigned char*) malloc(dataSize);
        memcpy(data, bytes + sizeof(short) + sizeof(offsets), dataSize);
        return Record({0, 0}, attributeCount, offsets, data);
    }

    unsigned char* Record::toBytes(u_short recordLength) {
        u_short dataSize = recordLength - sizeof(short) - sizeof(short) * attributeCount;
        unsigned char* byteArray = (unsigned char*) malloc(recordLength);
        memcpy(byteArray, &attributeCount, sizeof(short));
        memcpy(byteArray + sizeof(short), fieldOffsets, sizeof(short) * attributeCount);
        memcpy(byteArray + sizeof(short) + sizeof(short) * attributeCount, values, dataSize);
        return byteArray;
    }

    Record::~Record() = default;
}