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
        Record r = Record();
        r.populateMetadata(bytes);
        unsigned long dataSize = 0;
        int metadataSize = sizeof(short) + sizeof(short) * r.attributeCount;
        for (int i = r.attributeCount - 1; i >= 0; i--) {
            if (r.fieldOffsets[i] == -1) continue;
            dataSize = r.fieldOffsets[i] - metadataSize;
            break;
        }
        unsigned char* data = (unsigned char*) malloc(dataSize);
        memcpy(data, bytes + metadataSize, dataSize);
        r.values = data;
        return r;
    }

    void Record::populateMetadata(unsigned char* bytes){
        memcpy(&this->attributeCount, bytes, sizeof(short));
        this->fieldOffsets = (short *) malloc(sizeof(short) * attributeCount);
        memcpy(this->fieldOffsets, bytes + sizeof(short), sizeof(short) * attributeCount);
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