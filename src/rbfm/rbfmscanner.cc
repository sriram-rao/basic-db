#include "src/include/rbfm.h"
#include <utility>
#include <vector>
#include <string>
#include <algorithm>
#include <unordered_map>

namespace PeterDB {
    RBFM_ScanIterator::RBFM_ScanIterator(std::vector<Attribute> recordDescriptor, std::string conditionAttribute,
                                         CompOp compOp, void *value, std::vector<std::string> attributeNames,
                                         FileHandle& fileHandle) {
        this->recordDescriptor = recordDescriptor;
        this->conditionAttribute = conditionAttribute;
        this->compOp = compOp;
        this->value = value;
        this->attributeNames = attributeNames;
        this->pageNum = 0;
        this->slotNum = -1;
        this->fileHandle = fileHandle;
        // Call setFile to move the fstream
        // this->fileHandle.setFile(std::move(fileHandle.file));
    }

    void RBFM_ScanIterator::setFile(fstream &&file) {
        this->fileHandle.setFile(std::move(file));
    }

//    RBFM_ScanIterator & RBFM_ScanIterator::operator= (const RBFM_ScanIterator &other) {
//        this->recordDescriptor = other.recordDescriptor;
//        this->conditionAttribute = other.conditionAttribute;
//        this->compOp = other.compOp;
//        this->value = other.value;
//        this->attributeNames = other.attributeNames;
//        this->pageNum = other.pageNum;
//        this->slotNum = other.slotNum;
//        this->fileHandle = other.fileHandle;
//        return *this;
//    }

    RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) {
        Page page;
        page.directory.recordCount = 0;
        if (RecordBasedFileManager::readPage(pageNum, page, fileHandle) == -1)
            return RBFM_EOF;
        if (!incrementRid(page.directory.recordCount))
            return RBFM_EOF;

        rid = { pageNum, static_cast<unsigned short>(slotNum) };
        if (page.checkRecordDeleted(slotNum))
            return getNextRecord(rid, data);
        Record record;
        page.getRecord(slotNum, record);
        if (record.absent())
            return getNextRecord(rid, data);

        if (!conditionMet(record))
            return getNextRecord(rid, data);

        int recordNulls = ceil((float)recordDescriptor.size() / 8);
        int recordLength = 0;
        for (auto & attr : recordDescriptor) {
            recordLength += attr.length;
        }

        char* recordData = (char*) malloc(recordLength + recordNulls);
        RecordBasedFileManager::instance().readRecord(fileHandle, recordDescriptor, rid, recordData);

        // Null bitmap
        int nullBytes = ceil((float)attributeNames.size() / 8);
        int readOffset = ceil((float)recordDescriptor.size() / 8),
            writeOffset = nullBytes;
        int columnIndex = -1, currentIndex = -1;
        char nullBitMap [nullBytes];
        std::memset(nullBitMap, 0, nullBytes);

        // Project columns
        for (auto &attr : recordDescriptor) {
            columnIndex++;
            int fieldLength = 4;
            if (TypeVarChar == attr.type) {
                memcpy(&fieldLength, recordData + readOffset, sizeof(fieldLength));
                readOffset += sizeof(fieldLength);
            }

            if (std::find(attributeNames.begin(), attributeNames.end(), attr.name) == attributeNames.end()) {
                readOffset += fieldLength;
                continue; // column not needed
            }
            currentIndex++;
            if (record.offsets[currentIndex] == -1)
                nullBitMap[currentIndex / 8] = nullBitMap[currentIndex / 8] | (1 << (7 - currentIndex % 8));

            if (TypeVarChar == attr.type) {
                memcpy((char*)data + writeOffset, &fieldLength, sizeof(fieldLength));
                writeOffset += sizeof(fieldLength);
            }

            memcpy((char*)data + writeOffset, recordData + readOffset, fieldLength);
            writeOffset += fieldLength;
            readOffset += fieldLength;
        }
        memcpy(data, nullBitMap, nullBytes);
        free(recordData);

        return 0;
    }

    bool RBFM_ScanIterator::incrementRid(int recordCount) {
        if (recordCount >= slotNum + 2) {
            // If current page has more records, increment slot until we get one present in this page (un-deleted and un-moved)
            slotNum++;
            return true;
        }
        if (fileHandle.getNumberOfPages() > pageNum + 2) {
            // If all records of page are done, increment page and go to slot 0
            pageNum++;
            slotNum = 0;
            return true;
        }
        return false;
    }

    bool RBFM_ScanIterator::conditionMet(Record &record) {
        if (conditionAttribute.empty())
            return true;

        int length = 0;
        AttrType type = TypeInt;
        for (auto & i : recordDescriptor) {
            if (conditionAttribute == i.name) {
                length = i.length;
                type = i.type;
                break;
            }
        }
        char* data = (char*) malloc(length + 1);
        int readSuccess = RecordBasedFileManager::instance().readAttribute(record, recordDescriptor, { pageNum, static_cast<unsigned short>(slotNum) }, conditionAttribute, data);
        if (readSuccess != 0) {
            free(data);
            return false; // if attribute isn't present, default is that the record doesn't match
        }

        bool result = false;

        switch(compOp) {
            case EQ_OP:
                result = checkEqual(type, data, value);
                break;
            case LT_OP:
                result = checkLessThan(type, data, value);
                break;
            case LE_OP:
                result = checkEqual(type, data, value) || checkLessThan(type, data, value);
                break;
            case GT_OP:
                result = checkGreaterThan(type, data, value);
                break;
            case GE_OP:
                result = checkGreaterThan(type, data, value) || checkEqual(type, data, value);
                break;
            case NE_OP:
                result = !checkEqual(type, data, value);
                break;
            case NO_OP:
                result = true;
                break;
            default:
                break;
        }
        free(data);
        return result;
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
        int startOffset = 1;
        if (((char *) value1)[0] & (1 << 7))
            return false;
        if (TypeVarChar == type) {
            string s1 = RecordBasedFileManager::parseTypeVarchar(value1, startOffset);
            startOffset = 0;
            string s2 = RecordBasedFileManager::parseTypeVarchar(value2, startOffset);
            return s1 == s2;
        }

        if (TypeInt == type) {
            int n1 = RecordBasedFileManager::parseTypeInt(value1, startOffset, 4);
            startOffset = 0;
            int n2 = RecordBasedFileManager::parseTypeInt(value2, startOffset, 4);
            return n1 == n2;
        }

        float f1 = RecordBasedFileManager::parseTypeReal(value1, startOffset, 4);
        startOffset = 0;
        float f2 = RecordBasedFileManager::parseTypeReal(value2, startOffset, 4);
        return f1 == f2;
    }

    bool RBFM_ScanIterator::checkLessThan(AttrType type, const void *value1, const void *value2) {
        int startOffset = 1;
        if (((char *) value1)[0] & (1 << 7))
            return false;
        if (TypeVarChar == type) {
            string s1 = RecordBasedFileManager::parseTypeVarchar(value1, startOffset);
            startOffset = 0;
            string s2 = RecordBasedFileManager::parseTypeVarchar(value2, startOffset);
            return s1 < s2;
        }

        if (TypeInt == type) {
            int n1 = RecordBasedFileManager::parseTypeInt(value1, startOffset, 4);
            startOffset = 0;
            int n2 = RecordBasedFileManager::parseTypeInt(value2, startOffset, 4);
            return n1 < n2;
        }

        float f1 = RecordBasedFileManager::parseTypeReal(value1, startOffset, 4);
        startOffset = 0;
        float f2 = RecordBasedFileManager::parseTypeReal(value2, startOffset, 4);
        return f1 < f2;
    }

    bool RBFM_ScanIterator::checkGreaterThan(AttrType type, const void *value1, const void *value2) {
        int startOffset = 1;
        if (((char *) value1)[0] & (1 << 7))
            return false;
        if (TypeVarChar == type) {
            string s1 = RecordBasedFileManager::parseTypeVarchar(value1, startOffset);
            startOffset = 0;
            string s2 = RecordBasedFileManager::parseTypeVarchar(value2, startOffset);
            return s1 > s2;
        }

        if (TypeInt == type) {
            int n1 = RecordBasedFileManager::parseTypeInt(value1, startOffset, 4);
            startOffset = 0;
            int n2 = RecordBasedFileManager::parseTypeInt(value2, startOffset, 4);
            return n1 > n2;
        }

        float f1 = RecordBasedFileManager::parseTypeReal(value1, startOffset, 4);
        startOffset = 0;
        float f2 = RecordBasedFileManager::parseTypeReal(value2, startOffset, 4);
        return f1 > f2;
    }
}