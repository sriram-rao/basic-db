#include "src/include/rbfm.h"
#include <utility>
#include <vector>
#include <map>
#include <unordered_map>

namespace PeterDB {

    const unordered_map<int, compare> RBFM_ScanIterator::comparerMap = {
            { EQ_OP, &RBFM_ScanIterator::checkEqual },
            { LT_OP, &RBFM_ScanIterator::checkLessThan },
            { GT_OP, &RBFM_ScanIterator::checkGreaterThan },
            { LE_OP, [](AttrType type, const void* v1, const void* v2) -> bool { return checkLessThan(type, v1, v2) || checkEqual(type, v1, v2); } },
            { GE_OP, [](AttrType type, const void* v1, const void* v2) -> bool { return checkGreaterThan(type, v1, v2) || checkEqual(type, v1, v2); } },
            { NE_OP, [](AttrType type, const void* v1, const void* v2) -> bool { return !checkEqual(type, v1, v2); } },
            { NO_OP, [](AttrType type, const void* v1, const void* v2) -> bool { return true; } }
    };

    RBFM_ScanIterator::RBFM_ScanIterator(std::vector<Attribute> recordDescriptor, std::string conditionAttribute,
                                         CompOp compOp, void *value, std::vector<std::string> attributeNames,
                                         FileHandle& fileHandle) {
        this->recordDescriptor = std::move(recordDescriptor);
        this->conditionAttribute = std::move(conditionAttribute);
        this->compOp = compOp;
        this->value = value;
        this->attributeNames = std::move(attributeNames);
        this->currentRecord = { 0, 0 };
        this->fileHandle = fileHandle;
        this->fileHandle.setFile(std::move(fileHandle.file));
    }

    RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) {
        Page page = RecordBasedFileManager::readPage(currentRecord.pageNum, fileHandle);
        if (!incrementRid(page))
            return RBFM_EOF;

        rid = currentRecord;
        if (page.checkRecordDeleted(currentRecord.slotNum))
            return getNextRecord(rid, data);
        Record record = page.getRecord(currentRecord.slotNum);
        if (record.absent())
            return getNextRecord(rid, data);

        // Check conditions
        if (!conditionsMeet(record))
            return getNextRecord(rid, data);

        // readRecord
        RecordBasedFileManager::instance().readRecord(fileHandle, recordDescriptor, currentRecord, data);

        // TODO: projection

        return 0;
    }

    bool RBFM_ScanIterator::incrementRid(Page &page) {
        if (page.directory.recordCount > currentRecord.slotNum + 2) {
            // If current page has more records, increment slot until we get one present in this page (un-deleted and un-moved)
            currentRecord.slotNum++;
            return true;
        }
        if (fileHandle.getNumberOfPages() > currentRecord.pageNum) {
            // If all records of page are done, increment page and go to slot 0
            currentRecord.pageNum++;
            currentRecord.slotNum = 0;
            return true;
        }
        return false;
    }

    bool RBFM_ScanIterator::conditionsMeet(Record record) {
        int length = 0;
        AttrType type = TypeInt;
        for (auto & i : recordDescriptor) {
            if (conditionAttribute == i.name) {
                length = i.length;
                type = i.type;
                break;
            }
        }
        char* data = (char*) malloc(length);
        int readSuccess = RecordBasedFileManager::instance().readAttribute(record, recordDescriptor, currentRecord, conditionAttribute, data);
        if (!readSuccess)
            return false; // if attribute isn't present, default is that the record doesn't match
        return comparerMap.at(compOp)(type, value, data);
    }

    RC RBFM_ScanIterator::close() {
        // Get rid of state
        recordDescriptor.clear();
        conditionAttribute.clear();
        attributeNames.clear();
        fileHandle.close();
        return 0;
    }

    bool RBFM_ScanIterator::checkEqual(AttrType type, const void *value1, const void *value2) {
        string s1, s2;
        getStrings(type, value1, value2, s1, s2);
        if (TypeVarChar == type)
            return s1 == s2;

        float f1 = stof(s1), f2 = stof(s2);
        return f1 == f2;
    }

    bool RBFM_ScanIterator::checkLessThan(AttrType type, const void *value1, const void *value2) {
        string s1, s2;
        getStrings(type, value1, value2, s1, s2);
        if (TypeVarChar == type)
            return s1 < s2;

        float f1 = stof(s1), f2 = stof(s2);
        return f1 < f2;
    }

    bool RBFM_ScanIterator::checkGreaterThan(AttrType type, const void *value1, const void *value2) {
        string s1, s2;
        getStrings(type, value1, value2, s1, s2);
        if (TypeVarChar == type)
            return s1 > s2;

        float f1 = stof(s1), f2 = stof(s2);
        return f1 > f2;
    }

    void RBFM_ScanIterator::getStrings(AttrType type, const void* value1, const void* value2, string& s1, string& s2) {
        copy copyMethod = RecordBasedFileManager::parserMap.at(type);
        int startOffset = 0;
        s1 = copyMethod(value1, startOffset, 4);
        startOffset = 0;
        s2 = copyMethod(value2, startOffset, 4);
    }
}