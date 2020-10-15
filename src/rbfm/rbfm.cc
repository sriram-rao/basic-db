#include "src/include/rbfm.h"
#include <vector>

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
        short nullBytes = ceil(((float)recordDescriptor.size()) / 8);
        unsigned short recordLength = sizeof(short) + sizeof(short) * recordDescriptor.size(); // space for fieldCount 'n' and n offsets
        unsigned short offsets[recordDescriptor.size()];
        for (unsigned short i = 0; i < recordDescriptor.size(); ++i) {
            // TODO: fix this for varchar
            recordLength += recordDescriptor[i].length;
            offsets[i] = (i == 0 ? 0 : offsets[i-1]) + recordDescriptor[i].length;
        }

        // Find the right page
        unsigned short num = fileHandle.dataPageCount, slotNum = 0, pageDataSize = 0;
        short pageNum = fileHandle.findFreePage(recordLength + sizeof(offsets) + sizeof(short) + sizeof(Slot));
        Page p = pageNum >= 0 ? readPage(num, fileHandle) : Page();
        if (pageNum >= 0) {
            num = (unsigned short) pageNum;
            slotNum = p.directory.recordCount;
            for (unsigned short i = 0; i < p.directory.recordCount; ++i)
                pageDataSize += p.directory.getRecordLength(i);
        }
        rid = { num, slotNum };

        // Make the record
        unsigned char* fieldData = (unsigned char*)malloc(recordLength);
        memcpy(fieldData, (char*)data + nullBytes, recordLength);
        Record r = Record(rid, recordDescriptor.size(), offsets, fieldData);
        p.directory.addSlot(pageDataSize, recordLength);
        p.addRecord(r, recordLength);

        // Write to file
        writePage(num, p, fileHandle, pageNum == -1);
        return 0;
    }

    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {
        char* page = (char*) malloc(PAGE_SIZE);
        Page p = readPage(rid.pageNum, fileHandle);
        unsigned short recordOffset = p.directory.slots[rid.slotNum].offset,
            recordLength = p.directory.slots[rid.slotNum].length;
        memcpy(data, page + recordOffset, recordLength);

        // TODO: Format data here


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
                    memoryPointerCounter = memoryPointerCounter + sizeof(TypeInt);
                    records.append(recordDescriptor[i].name);
                    records.append(": ");
                    records.append(std::to_string(tmpIntVal));
                } else if (recordDescriptor[i].type == TypeReal) {
                    std::memcpy(&tmpFloatVal, (char *) data + memoryPointerCounter, recordDescriptor[i].length);
                    memoryPointerCounter = memoryPointerCounter + sizeof(TypeReal);
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
        short recordCount, freeBytes;
        memcpy(&recordCount, pageData + PAGE_SIZE - sizeof(short), sizeof(short));
        memcpy(&freeBytes, pageData + PAGE_SIZE - sizeof(short) * 2, sizeof(short));
        size_t slotSize = sizeof(Slot) * recordCount;
        Slot slots[recordCount];
        memcpy(slots, pageData + PAGE_SIZE - sizeof(short) * 2 - slotSize, slotSize);
        SlotDirectory dir = SlotDirectory(freeBytes, recordCount, slots);

        // Read records as array
        u_short recordsSize = PAGE_SIZE - sizeof(short) * 2 - slotSize - freeBytes;
        unsigned char* records = (unsigned char*) malloc(recordsSize);
        memcpy(records, pageData, recordsSize); // TODO: check
        free(pageData);
        return Page(dir, records);
    }

    RC RecordBasedFileManager::writePage(PageNum pageNum, Page &page, FileHandle &file, bool toAppend) {
        char* pageData = (char*) malloc(PAGE_SIZE);
        short slotsSize = sizeof(Slot) * page.directory.recordCount;
        unsigned short recordsSize = 0;
        for (short i = 0; i < page.directory.recordCount; ++i)
            recordsSize = page.directory.getRecordLength(i);

        memcpy(pageData, page.records, recordsSize);
        short freeBytes = PAGE_SIZE - recordsSize - sizeof(short) * 2 - slotsSize;
        file.setPageSpace(pageNum, freeBytes);
//        memcpy(pageData + recordsSize + freeBytes, page.directory.slots, slotsSize);
        memcpy(pageData + PAGE_SIZE - sizeof(short) * 2 - slotsSize, page.directory.slots, slotsSize);
        memcpy(pageData + PAGE_SIZE - sizeof(short) * 2, &freeBytes, sizeof(short));
        memcpy(pageData + PAGE_SIZE - sizeof(short), &page.directory.recordCount, sizeof(short));

        return toAppend ? file.appendPage(pageData) : file.writePage(pageNum, pageData);
    }
} // namespace PeterDB

