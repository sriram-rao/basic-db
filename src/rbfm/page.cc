#include "src/include/rbfm.h"

#define RECORD_BATCH 5

using namespace std;

namespace PeterDB {
    Page::Page() {

    }

    Page::Page(SlotDirectory directory, Record* records) {
        this->directory = directory;
        this->records = records;
    }

    RC Page::addRecord(Record record, unsigned short recordLength) {
        short recordCount = this->directory.recordCount;
        if (recordCount % RECORD_BATCH == 0) {
            Record* newSet = (Record *)malloc(sizeof(Record) * (recordCount + RECORD_BATCH));
            std::copy(this->records, this->records + recordCount, newSet);
            delete [] this->records;
            this->records = newSet;
        }
        this->records[recordCount] = record;
        this->directory.recordCount = this->directory.recordCount + 1;
        this->directory.freeSpace = this->directory.freeSpace - recordLength - sizeof(Slot);
        return 0;
    }

    Page::~Page() = default;
}
