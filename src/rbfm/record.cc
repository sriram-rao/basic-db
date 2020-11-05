#include "src/include/rbfm.h"
#include <cstring>

namespace PeterDB {
    Record::Record() {
        this->attributeCount = 0;
        this->rid = { 0, 0 };
        this->offsets = vector<short>(0);
        this->values = nullptr;
    }

    Record::Record(RID newId, short countOfAttributes, vector<short> newOffsets, char* newValues) {
        this->rid = newId;
        this->attributeCount = countOfAttributes;
        this->offsets = newOffsets;

        int dataSize = 0;
        int offsetCount = countOfAttributes == -1 ? 2 : countOfAttributes;
        int metadataSize = sizeof(short) + sizeof(short) * offsetCount;
        for (int i = offsetCount - 1; i >= 0; i--) {
            if (newOffsets[i] == -1) continue;
            dataSize = newOffsets[i] - metadataSize;
            break;
        }
        this->values = nullptr;
        if (dataSize > 0) {
            this->values = (char *) malloc(dataSize);
            memcpy(this->values, newValues, dataSize);
        }
    }

    Record::Record(char* bytes) {
        this->populateMetadata(bytes);
        this->populateData(bytes);
    }

    Record & Record::operator=(const Record &other) {
        this->rid = other.rid;
        this->attributeCount = other.attributeCount;
        this->offsets = other.offsets;

        int dataSize = 0;
        int offsetCount = other.attributeCount == -1 ? 2 : other.attributeCount;
        int metadataSize = sizeof(short) + sizeof(short) * offsetCount;
        for (int i = offsetCount - 1; i >= 0; i--) {
            if (other.offsets[i] == -1) continue;
            dataSize = other.offsets[i] - metadataSize;
            break;
        }
        free(this->values);
        this->values = (char *) malloc(dataSize);
        memcpy(this->values, other.values, dataSize);
        return *this;
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
        int offsetCount = this->attributeCount == -1 ? 2 : this->attributeCount;
        int metadataSize = sizeof(short) + sizeof(short) * offsetCount;
        int startOffset = metadataSize;
        for (int i = index - 1; i >= 0; i--) {
            if (offsets[i] == -1) continue;
            startOffset = offsets[i];
            break;
        }
        memcpy(data, values + startOffset - metadataSize, endOffset - startOffset);
        return true;
    }

    void Record::populateMetadata(char* bytes){
        memcpy(&this->attributeCount, bytes, sizeof(short));
        int offsetCount = this->attributeCount == -1 ? 2 : this->attributeCount;
        offsets = vector<short>(offsetCount, 0);
        memcpy(offsets.data(), bytes + sizeof(short), sizeof(short) * offsetCount);
    }

    void Record::populateData(char* bytes){
        // Assumes metadata has been populated
        unsigned long dataSize = 0;
        int offsetCount = this->attributeCount == -1 ? 2 : this->attributeCount;
        int metadataSize = sizeof(short) + sizeof(short) * offsetCount;
        for (int i = offsetCount - 1; i >= 0; i--) {
            if (offsets[i] == -1) continue;
            dataSize = offsets[i] - metadataSize;
            break;
        }
        this->values = (char*) malloc(dataSize);
        memcpy(this->values, bytes + metadataSize, dataSize);
    }

    void Record::toBytes(u_short recordLength, char* bytes) {
        int fieldCount = this->attributeCount == -1 ? 2 : this->attributeCount;
        unsigned long dataSize = 0;
        int offsetCount = this->attributeCount == -1 ? 2 : this->attributeCount;
        int metadataSize = sizeof(short) + sizeof(short) * offsetCount;
        for (int i = offsetCount - 1; i >= 0; i--) {
            if (offsets[i] == -1) continue;
            dataSize = offsets[i] - metadataSize;
            break;
        }
        memcpy(bytes, &this->attributeCount, sizeof(short));
        memcpy(bytes + sizeof(short), offsets.data(), sizeof(short) * fieldCount);
        memcpy(bytes + sizeof(short) + sizeof(short) * fieldCount, values, dataSize);
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

    Record::~Record() {
        free(values);
    }
}