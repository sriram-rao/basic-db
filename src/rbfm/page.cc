#include "src/include/rbfm.h"
#include <cstring>

using namespace std;

namespace PeterDB {
    Page::Page() {
        this->records = (char*) malloc(PAGE_SIZE);
        this->directory = SlotDirectory(PAGE_SIZE - sizeof(short) * 2, 0);
    }

    Page& Page::operator=(const Page &other) {
        this->directory = other.directory;
        memcpy(this->records, other.records, PAGE_SIZE);
        return *this;
    }

    RC Page::addRecord(unsigned short slotNum, Record &record, unsigned short recordLength) {
        // To set to the start of the record
        Slot leftSlot = findFilledSlotBetween(0, slotNum - 1);
        int copiedLength = leftSlot.length > 0 ? leftSlot.offset + leftSlot.length : 0;

        // To copy data after slot with addition
        Slot rightSlot = findFilledSlotBetween(slotNum + 1, directory.slots.size() - 1);
        if (rightSlot.length > 0) {
            memcpy(this->records + copiedLength + recordLength, records + leftSlot.offset + leftSlot.length,
                   rightSlot.offset + rightSlot.length - (leftSlot.offset + leftSlot.length));
            for (int i = slotNum + 1; i < directory.recordCount; ++i)
                if (directory.slots[i].offset != -1)
                    directory.slots[i].offset += recordLength;
        }

        char *recordBytes = (char *) malloc(recordLength);
        record.toBytes(recordLength, recordBytes);
        memcpy(this->records + copiedLength, recordBytes, recordLength);
        free(recordBytes);
        this->directory.freeSpace = this->directory.freeSpace - recordLength;
        return 0;
    }

    RC Page::updateRecord(unsigned short slotNum, Record &record, short newLength) {
        // Shift records to the right
        if (slotNum + 1 < directory.recordCount) {
            Slot lastSlot = findFilledSlotBetween(slotNum + 1, directory.recordCount - 1);
            Slot currentSlot = directory.slots[slotNum];
            int shiftSize = lastSlot.offset + lastSlot.length - (currentSlot.offset + currentSlot.length);
            moveRecords(currentSlot.offset + currentSlot.length,
                        currentSlot.offset + newLength, shiftSize);

            for (int i = slotNum + 1; i < directory.recordCount; ++i)
                if (directory.slots[i].offset != -1)
                    directory.slots[i].offset += (newLength - currentSlot.length);
        }
        // Update record in place
        char *recordBytes = (char *) malloc(newLength);
        record.toBytes(newLength, recordBytes);
        memcpy(records + directory.getRecordOffset(slotNum), recordBytes, newLength);
        free(recordBytes);
        directory.freeSpace -= (newLength - directory.getRecordLength(slotNum));
        directory.updateSlot(slotNum, newLength);
        return 0;
    }

    RC Page::deleteRecord(unsigned short slotNum) {
        // Get record length (including offsets)
        Slot recordSlot = this->directory.slots[slotNum];

        // Are there records to the right?
        if (directory.recordCount > slotNum + 1) {
            Slot lastSlot = findFilledSlotBetween(0, directory.recordCount - 1);
            int shiftDataSize = lastSlot.offset + lastSlot.length - (recordSlot.offset + recordSlot.length);
            moveRecords(recordSlot.offset + recordSlot.length, recordSlot.offset, shiftDataSize);

            // Update offsets of moved records
            for (int i = slotNum + 1; i < directory.recordCount; ++i)
                if (directory.slots[i].offset != -1)
                    directory.slots[i].offset -= recordSlot.length;

        }

        // Update deleted records slot information to -1 in all the slots referring to it
        this->directory.freeSpace += recordSlot.length;
        this->directory.slots[slotNum].length = -1;
        this->directory.slots[slotNum].offset = -1;

        return 0;
    }

    Record Page::getRecord(unsigned short slotNum) {
        if (checkRecordDeleted(slotNum))
            return Record({ PAGE_SIZE * 2, PAGE_SIZE  * 2 }, -1, vector<short>(0), nullptr);

        Slot recordSlot = this->directory.slots[slotNum];
        char* recordData = (char*) malloc(recordSlot.length); // Need to free
        memcpy(recordData, records + recordSlot.offset, recordSlot.length);
        return Record(recordData);
    }

    bool Page::checkRecordDeleted(unsigned short slotNum) {
        return this->directory.slots[slotNum].length == -1;
    }

    unsigned short Page::getFreeSlot() {
        unsigned short slotNum = 0;
        for (; slotNum < this->directory.recordCount; ++slotNum) {
            if (this->directory.slots[slotNum].length == -1)
                return slotNum;
        }
        return slotNum;
    }

    void Page::moveRecords(int moveStartOffset, int destinationOffset, int length) {
        char *dataToShift = (char *) malloc(length);
        std::memcpy(dataToShift, this->records + moveStartOffset, length);
        std::memcpy(this->records + destinationOffset, dataToShift, length);
        free(dataToShift);
    }

    int Page::getDataRecordCount() {
        int count = 0;
        for (int i = 0; i < directory.recordCount; ++i)
            if (directory.slots[i].length != -1)
                count++;
        return count;
    }

    bool Page::checkValid() {
        return !(this->directory.recordCount == 0);
    }

    Slot Page::findFilledSlotBetween(int startSlot, int endSlot) {
        for (int i = endSlot; i >= startSlot; i--)
            if (this->directory.slots[i].length != -1)
                return this->directory.slots[i];

        return { 0, 0 };
    }

    Page::~Page() {
        free(this->records);
    }
}
