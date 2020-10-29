#include "src/include/rbfm.h"
#include <cstring>

namespace PeterDB {
    Record::Record() = default;

    Record::Record(RID id, short countOfAttributes, vector<short> offsets, unsigned char* values) {
        this->rid = id;
        this->attributeCount = countOfAttributes;
        this->offsets = offsets;
        this->values = values;
    }

    int Record::getAttributeLength(int index) {
        int endOffset = offsets[index];
        if (endOffset == -1)
            return -1;
        int startOffset = 0;
        for (int i = index - 1; i >= 0; i--) {
            if (offsets[i] == -1) continue;
            startOffset = offsets[i];
            break;
        }
        return endOffset - startOffset;
    }

    bool Record::readAttribute(int index, void* data) {
        int endOffset = offsets[index];
        int startOffset = 0;
        for (int i = index - 1; i >= 0; i--) {
            if (offsets[i] == -1) continue;
            startOffset = offsets[i];
            break;
        }
        data = malloc(endOffset - startOffset);
        memcpy(data, values + startOffset, endOffset - startOffset);
        return true;
    }

    Record Record::fromBytes(unsigned char* bytes) {
        Record r = Record();
        r.populateMetadata(bytes);
        r.populateData(bytes);
        return r;
    }

    void Record::populateMetadata(unsigned char* bytes){
        memcpy(&this->attributeCount, bytes, sizeof(short));
        int offsetCount = this->attributeCount == -1 ? 2 : this->attributeCount;
        offsets = vector<short>(offsetCount, 0);
        memcpy(offsets.data(), bytes + sizeof(short), sizeof(short) * offsetCount);
    }

    void Record::populateData(unsigned char* bytes){
        // Assumes metadata has been populated
        unsigned long dataSize = 0;
        int offsetCount = this->attributeCount == -1 ? 2 : this->attributeCount;
        int metadataSize = sizeof(short) + sizeof(short) * offsetCount;
        for (int i = offsetCount - 1; i >= 0; i--) {
            if (offsets[i] == -1) continue;
            dataSize = offsets[i] - metadataSize;
            break;
        }
        unsigned char* data = (unsigned char*) malloc(dataSize);
        memcpy(data, bytes + metadataSize, dataSize);
        this->values = data;
    }

    unsigned char* Record::toBytes(u_short recordLength) {
        int attributeCount = this->attributeCount == -1 ? 2 : this->attributeCount;
        u_short dataSize = recordLength - sizeof(short) - sizeof(short) * attributeCount;
        unsigned char* byteArray = (unsigned char*) malloc(recordLength);
        memcpy(byteArray, &this->attributeCount, sizeof(short));
        memcpy(byteArray + sizeof(short), offsets.data(), sizeof(short) * attributeCount);
        memcpy(byteArray + sizeof(short) + sizeof(short) * attributeCount, values, dataSize);
        return byteArray;
    }

    bool Record::absent() const {
        return (this->attributeCount == -1);
    }

    RID Record::getNewRid() const {
        if (!absent()) throw std::exception(); // Method not usable if the record is present
        unsigned pageNum;
        unsigned short slotNum;
        std::memcpy(&pageNum, this->values, sizeof(pageNum));
        std::memcpy(&slotNum, this->values + sizeof(pageNum), sizeof(slotNum));

        return { pageNum, slotNum };
    }

    Record::~Record() = default;
}