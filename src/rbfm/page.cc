#include "src/include/rbfm.h"

using namespace std;

namespace PeterDB {
    Page::Page() {
        this->records = (unsigned char*) malloc(sizeof(unsigned char));
        vector<Slot> slots;
        this->directory = SlotDirectory(PAGE_SIZE - sizeof(Slot) - sizeof(unsigned char), 0, slots);
    }

    Page::Page(SlotDirectory &directory, unsigned char* records) {
        this->directory = directory;
        this->records = records;
    }

    RC Page::addRecord(Record record, unsigned short recordLength) {
        short dataSize = this->directory.recordCount == 0 ? (short)0 : PAGE_SIZE - this->directory.freeSpace - sizeof(short) * 2 - sizeof(Slot) * this->directory.recordCount;
        unsigned char* newBytes = (unsigned char *) malloc(dataSize + recordLength);
        memcpy(newBytes, this->records, dataSize);
        memcpy(newBytes + dataSize, record.toBytes(recordLength), recordLength);
        free(this->records);
        this->records = newBytes;
        this->directory.recordCount++;
        this->directory.freeSpace = this->directory.freeSpace - recordLength;
        return 0;
    }

    Page::~Page() = default;
}
