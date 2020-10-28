#include "src/include/rbfm.h"
#include <cstring>

using namespace std;

namespace PeterDB {
    Page::Page() {
        this->records = (unsigned char*) malloc(sizeof(unsigned char));
        vector<Slot> slots;
        this->directory = SlotDirectory(PAGE_SIZE - sizeof(short) * 2, 0, slots);
    }

    Page::Page(SlotDirectory &directory, unsigned char* records) {
        this->directory = directory;
        this->records = records;
    }

    RC Page::addRecord(unsigned short slotNum, Record record, unsigned short recordLength) {
        short dataSize = getDataRecordCount() == 0 ? (short)0 : PAGE_SIZE - sizeof(short) * 2 -
                sizeof(Slot) * this->directory.slots.size() - this->directory.freeSpace;
        unsigned char* newBytes = (unsigned char *) malloc(dataSize + recordLength);

        // To copy data to the left of the record
        Slot leftSlot = findFilledSlotBetween(0, slotNum - 1);
        int copiedLength = 0;
        if (leftSlot.length > 0) {
            memcpy(newBytes, this->records, leftSlot.offset + leftSlot.length);
            copiedLength += leftSlot.offset + leftSlot.length;
        }
        memcpy(newBytes + copiedLength, record.toBytes(recordLength), recordLength);
        copiedLength += recordLength;

        // To copy data after slot with addition
        Slot rightSlot = findFilledSlotBetween(slotNum + 1, directory.slots.size() - 1);
        if (rightSlot.length > 0)
            memcpy(newBytes + copiedLength, records + leftSlot.offset + leftSlot.length,
               rightSlot.offset + rightSlot.length - (leftSlot.offset + leftSlot.length));

        free(this->records);
        this->records = newBytes;
        this->directory.freeSpace = this->directory.freeSpace - recordLength;
        return 0;
    }

    RC Page::updateRecord(unsigned short slotNum, Record record, short newLength) {
        // Shift records to the right
        if (slotNum + 1 < directory.recordCount) {
            Slot lastSlot = findFilledSlotBetween(slotNum + 1, directory.recordCount - 1);
            Slot currentSlot = directory.slots[slotNum];
            int shiftSize = lastSlot.offset + lastSlot.length - (currentSlot.offset + currentSlot.length);
            moveRecords(currentSlot.offset + currentSlot.length,
                        currentSlot.offset + newLength, shiftSize);

            for (int i = slotNum + 1; i < directory.recordCount; ++i)
                directory.slots[i].offset += (newLength - currentSlot.length);
        }
        // Update record in place
        memcpy(records + directory.getRecordOffset(slotNum), record.toBytes(newLength), newLength);
        directory.freeSpace += (newLength - directory.getRecordLength(slotNum));
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
        unsigned char* recordData = (unsigned char*) malloc(recordSlot.length);
        memcpy(recordData, records + recordSlot.offset, recordSlot.length);
        return Record::fromBytes(recordData);
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
        if (destinationOffset > moveStartOffset)
        {
            short dataSize = getDataRecordCount() == 0 ? (short)0 :
                    PAGE_SIZE - sizeof(short) * 2 - sizeof(Slot) * directory.slots.size() - directory.freeSpace;
            unsigned char* newBytes = (unsigned char *) malloc(dataSize + destinationOffset - moveStartOffset);
            memcpy(newBytes, records, dataSize);
            free(records);
            records = newBytes;
        }
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

    Page::~Page() = default;
}
