#include "src/include/rbfm.h"
#include <vector>
#include <map>
#include <cstring>

using namespace std;

namespace PeterDB {
    RecordBasedFileManager &RecordBasedFileManager::instance() {
        static RecordBasedFileManager _rbf_manager = RecordBasedFileManager();
        return _rbf_manager;
    }

    RecordBasedFileManager::RecordBasedFileManager() = default;

    RecordBasedFileManager::~RecordBasedFileManager() = default;

    RecordBasedFileManager::RecordBasedFileManager(const RecordBasedFileManager &) = default;

    RecordBasedFileManager &RecordBasedFileManager::operator=(const RecordBasedFileManager &) = default;

    RC RecordBasedFileManager::createFile(const std::string &fileName) {
        PagedFileManager &pagedFileManager = PagedFileManager::instance();
        return pagedFileManager.createFile(fileName);
    }

    RC RecordBasedFileManager::destroyFile(const std::string &fileName) {
        PagedFileManager &pagedFileManager = PagedFileManager::instance();
        return pagedFileManager.destroyFile(fileName);
    }

    RC RecordBasedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        PagedFileManager &pagedFileManager = PagedFileManager::instance();
        return pagedFileManager.openFile(fileName, fileHandle);
    }

    RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
        PagedFileManager &pagedFileManager = PagedFileManager::instance();
        return pagedFileManager.closeFile(fileHandle);
    }

    RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, RID &rid) {
        // Figure out record details
        // Null map
        short nullBytes = ceil(((float)recordDescriptor.size()) / 8);
        int fieldInfo[recordDescriptor.size()];

        u_short recordLength = sizeof(short) + sizeof(short) * recordDescriptor.size(); // space for fieldCount 'n' and n offsets
        short dataOffset = sizeof(short) * (recordDescriptor.size() + 1);
        short offsets[recordDescriptor.size()];
        int currentOffset = nullBytes;
        for (short i = 0; i < recordDescriptor.size(); ++i) {
            // check null
            if (((char*)data)[i / 8] & (1 << (7 - i % 8))) {
                offsets[i] = -1;
                fieldInfo[i] = -1;
                continue;
            }

            // get size for varchar
            int fieldSize = recordDescriptor[i].type == TypeVarChar ? 0 : recordDescriptor[i].length;
            if (recordDescriptor[i].type == TypeVarChar) {
                int varcharSize = 0;
                memcpy(&varcharSize, (char *) data + currentOffset, 4);
                fieldSize = varcharSize;
                currentOffset += 4;
            }
            recordLength += fieldSize;
            fieldInfo[i] = fieldSize;
            dataOffset += fieldSize;
            offsets[i] = dataOffset;
            currentOffset += fieldSize;
        }

        // Make the record
        unsigned char* fieldData = (unsigned char*)malloc(recordLength);
        currentOffset = nullBytes;
        int copiedOffset = 0;
        for(int i = 0; i < recordDescriptor.size(); i++) {
            if (offsets[i] == -1) // skip nulls
                continue;
            if (recordDescriptor[i].type == TypeVarChar) { // handle length descriptor for varchar
                currentOffset += 4;
            }
            memcpy(fieldData + copiedOffset, (char *) data + currentOffset, fieldInfo[i]);
            currentOffset += fieldInfo[i];
            copiedOffset += fieldInfo[i];
        }
        Record r = Record(rid, recordDescriptor.size(), offsets, fieldData);

        // Find the right page
        unsigned num = fileHandle.dataPageCount, pageDataSize = 0;
        unsigned short slotNum = 0;
        short pageNum = fileHandle.findFreePage(recordLength + sizeof(offsets) + sizeof(short) + sizeof(Slot));
        Page p = pageNum >= 0 ? readPage(pageNum, fileHandle) : Page();
        if (pageNum >= 0) {
            num = (unsigned short) pageNum;
            slotNum = static_cast<unsigned short>(p.directory.recordCount);
            for (unsigned short i = 0; i < p.directory.recordCount; ++i)
                pageDataSize += p.directory.getRecordLength(i);
        }
        rid.pageNum = num;
        rid.slotNum = slotNum;

        p.directory.addSlot(pageDataSize, recordLength);
        p.addRecord(r, recordLength);

        // Write to file
        writePage(num, p, fileHandle, pageNum == -1);
        return 0;
    }

    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {
        Page page = readPage(rid.pageNum, fileHandle);
        short recordOffset = page.directory.slots[rid.slotNum].offset,
            recordLength = page.directory.slots[rid.slotNum].length;
        unsigned char* recordData = (unsigned char*) malloc(recordLength);
        memcpy(recordData, page.records + recordOffset, recordLength);

        Record record = Record::fromBytes(recordData);

        // Null bitmap
        int nullBytes = ceil((float)record.attributeCount / 8);
        char nullBitMap [nullBytes];
        for (int i = 0; i < nullBytes; ++i)
            nullBitMap[i] = 0;
        for (int i = 0; i < recordDescriptor.size(); ++i) {
            if (record.fieldOffsets[i] == -1)
                nullBitMap[i / 8] = nullBitMap[i / 8] | (1 << (7 - i % 8));
        }

        memcpy(data, nullBitMap, nullBytes);
        int currentOffset = nullBytes;
        int sourceOffset = 0;
        // Add back varchar length
        for (int i = 0; i < recordDescriptor.size(); ++i) {
            int fieldSize = recordDescriptor[i].length;
            if (recordDescriptor[i].type == TypeVarChar) {
                // Get the size of the varchar and append
                int startIndex = i == 0 ? sizeof(short) * (recordDescriptor.size() + 1) : record.fieldOffsets[i - 1];
                fieldSize = record.fieldOffsets[i] - startIndex;
                memcpy((char*) data + currentOffset, &fieldSize, 4);
                currentOffset += 4;
            }
            memcpy((char*)data + currentOffset, record.values + sourceOffset, fieldSize);
            currentOffset += fieldSize;
            sourceOffset += fieldSize;
        }

        return 0;
    }

    RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data,
                                           std::ostream &out) {
        static_assert(sizeof(float) == 4, "");
        int flagBitsCount = ceil(((float) recordDescriptor.size()) / 8);
        std::vector<bool> nullFlagStatus;
        int memoryPointerCounter = flagBitsCount;
        unsigned short stringSizeOffsetMemSize = 4;

        std::string records;
        int tmpIntVal;
        float tmpFloatVal;

        for(unsigned int i = 0; i < recordDescriptor.size(); ++i) {
            if (((char*)data)[i / 8] & (1 << (7 - i % 8))) {
                records.append(recordDescriptor[i].name);
                records.append(": NULL");
            } else {
                if (recordDescriptor[i].type == TypeInt) {
                    std::memcpy(&tmpIntVal, (char *) data + memoryPointerCounter, recordDescriptor[i].length);
                    memoryPointerCounter = memoryPointerCounter + recordDescriptor[i].length;
                    records.append(recordDescriptor[i].name);
                    records.append(": ");
                    records.append(std::to_string(tmpIntVal));
                } else if (recordDescriptor[i].type == TypeReal) {
                    std::memcpy(&tmpFloatVal, (char *) data + memoryPointerCounter, recordDescriptor[i].length);
                    memoryPointerCounter = memoryPointerCounter + recordDescriptor[i].length;
                    records.append(recordDescriptor[i].name);
                    records.append(": ");
                    std::ostringstream ss;
                    ss << tmpFloatVal;
                    records.append(ss.str());
                } else if (recordDescriptor[i].type == TypeVarChar) {
                    std::memcpy(&tmpIntVal, (char *) data + memoryPointerCounter, stringSizeOffsetMemSize);
                    char *tmpStringVal = new char[tmpIntVal + 2];
                    std::memcpy(tmpStringVal, (char *) data + memoryPointerCounter + stringSizeOffsetMemSize,
                                tmpIntVal);
                    tmpStringVal[tmpIntVal] = '\0';
                    memoryPointerCounter = memoryPointerCounter + stringSizeOffsetMemSize + tmpIntVal;
                    records.append(recordDescriptor[i].name);
                    records.append(": ");
                    records.append(tmpStringVal);
                }
            }
            if (i != recordDescriptor.size() - 1) {
                records.append(", ");
            } else {
                // TODO :: is this required?
//                records.append("\n");
            }
        }
        out << records;
        return 0;
    }

    RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const RID &rid) {
        return -1;
    }

    RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, const RID &rid) {
        return -1;
    }

    RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                             const RID &rid, const std::string &attributeName, void *data) {
        return -1;
    }

    RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                    const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                    const std::vector<std::string> &attributeNames,
                                    RBFM_ScanIterator &rbfm_ScanIterator) {
        return -1;
    }

    Page RecordBasedFileManager::readPage(PageNum pageNum, FileHandle &file) {
        char* pageData = (char*) malloc(PAGE_SIZE);
        file.readPage(pageNum, pageData);

        // Read slot directory
        short recordCount;
        short freeBytes;
        memcpy(&recordCount, pageData + PAGE_SIZE - sizeof(short), sizeof(short));
        memcpy(&freeBytes, pageData + PAGE_SIZE - sizeof(short) * 2, sizeof(short));
        size_t slotSize = sizeof(Slot) * recordCount;
        vector<Slot> slots;
        slots.insert(slots.begin(), recordCount, { 0, 0 });
        memcpy(slots.data(), pageData + PAGE_SIZE - sizeof(short) * 2 - slotSize, slotSize);
        SlotDirectory dir = SlotDirectory(freeBytes, recordCount, slots);

        // Read records as array
        u_short recordsSize = PAGE_SIZE - sizeof(short) * 2 - slotSize - freeBytes;
        unsigned char* records = (unsigned char*) malloc(recordsSize);
        memcpy(records, pageData, recordsSize);
        free(pageData);
        return Page(dir, records);
    }

    RC RecordBasedFileManager::writePage(PageNum pageNum, Page &page, FileHandle &file, bool toAppend) {
        char* pageData = (char*) malloc(PAGE_SIZE);
        short slotsSize = sizeof(Slot) * page.directory.slots.size();
        unsigned short recordsSize = 0;
        for (short i = 0; i < page.directory.recordCount; ++i)
            recordsSize += page.directory.getRecordLength(i);

        memcpy(pageData, page.records, recordsSize);
        short freeBytes = PAGE_SIZE - recordsSize - sizeof(short) * 2 - slotsSize;
        memcpy(pageData + PAGE_SIZE - sizeof(short) * 2 - slotsSize, page.directory.slots.data(), slotsSize);
        memcpy(pageData + PAGE_SIZE - sizeof(short) * 2, &freeBytes, sizeof(short));
        memcpy(pageData + PAGE_SIZE - sizeof(short), &page.directory.recordCount, sizeof(short));
        RC writeSuccess = toAppend ? file.appendPage(pageData) : file.writePage(pageNum, pageData);
        file.setPageSpace(pageNum, freeBytes);

        return writeSuccess;
    }
} // namespace PeterDB

