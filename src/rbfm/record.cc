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

    void Record::readAttribute(int index, void* data) {
        int endOffset = fieldOffsets[index];
        int startOffset = 0;
        for (int i = index - 1; i >= 0; i--) {
            if (fieldOffsets[i] == -1) continue;
            startOffset = fieldOffsets[i];
            break;
        }
        memcpy(data, values + startOffset, endOffset - startOffset);
    }

    Record Record::fromBytes(unsigned char* bytes) {
        Record r = Record();
        r.populateMetadata(bytes);
        r.populateData(bytes);
        return r;
    }

    void Record::populateMetadata(unsigned char* bytes){
        memcpy(&this->attributeCount, bytes, sizeof(short));
        this->fieldOffsets = (short *) malloc(sizeof(short) * attributeCount);
        memcpy(this->fieldOffsets, bytes + sizeof(short), sizeof(short) * attributeCount);
    }

    void Record::populateData(unsigned char* bytes){
        // Assumes metadata has been populated
        unsigned long dataSize = 0;
        int metadataSize = sizeof(short) + sizeof(short) * this->attributeCount;
        for (int i = this->attributeCount - 1; i >= 0; i--) {
            if (this->fieldOffsets[i] == -1) continue;
            dataSize = this->fieldOffsets[i] - metadataSize;
            break;
        }
        unsigned char* data = (unsigned char*) malloc(dataSize);
        memcpy(data, bytes + metadataSize, dataSize);
        this->values = data;
    }

    unsigned char* Record::toBytes(u_short recordLength) {
        u_short dataSize = recordLength - sizeof(short) - sizeof(short) * attributeCount;
        unsigned char* byteArray = (unsigned char*) malloc(recordLength);
        memcpy(byteArray, &attributeCount, sizeof(short));
        memcpy(byteArray + sizeof(short), fieldOffsets, sizeof(short) * attributeCount);
        memcpy(byteArray + sizeof(short) + sizeof(short) * attributeCount, values, dataSize);
        return byteArray;
    }

    bool Record::absent() const {
        return (this->attributeCount == -1);
    }

    RID Record::getNewRid() const {
        if (!absent()) throw std::exception(); // Method not usable if the record is present
        int ridStart = sizeof(int16_t) * 3; // attribute count, two offsets
        unsigned pageNum;
        unsigned short slotNum;
        std::memcpy(&pageNum, this->values + ridStart, sizeof(pageNum));
        std::memcpy(&slotNum, this->values + ridStart + sizeof(pageNum), sizeof(slotNum));

        return { pageNum, slotNum };
    }

    Record::~Record() = default;
}