#include "src/include/pfm.h"
#include <cstdio>

namespace PeterDB {
    PagedFileManager &PagedFileManager::instance() {
        static PagedFileManager _pf_manager = PagedFileManager();
        return _pf_manager;
    }

    PagedFileManager::PagedFileManager() = default;

    PagedFileManager::~PagedFileManager() = default;

    PagedFileManager::PagedFileManager(const PagedFileManager &) = default;

    PagedFileManager &PagedFileManager::operator=(const PagedFileManager &) = default;

    RC PagedFileManager::createFile(const std::string &fileName) {
        if (FileHandle::exists(fileName)) return -1;
        FILE* newFile;
        newFile = fopen(fileName.c_str(), "w+b");
        FileHandle::init(newFile);
        fclose(newFile);
        return 0;
    }

    RC PagedFileManager::destroyFile(const std::string &fileName) {
        return remove(fileName.c_str());
    }

    RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        if (fileHandle.isOpen()) return -1;
        bool newFile = !FileHandle::exists(fileName);
        FILE* f = fopen(fileName.c_str(), "r+b");
        fileHandle = FileHandle(f);
        if (newFile)
            FileHandle::init(f);
        else
            fileHandle.openFile();
        return 0;
    }

    RC PagedFileManager::closeFile(FileHandle &fileHandle) {
        fileHandle.closeFile();
        fclose(fileHandle.file);
        return 0;
    }

    FileHandle::FileHandle() {
        readPageCounter = 0;
        writePageCounter = 0;
        appendPageCounter = 0;
        dataPageCount = 0;
        file = NULL;
    }

    FileHandle::FileHandle(FILE* f) {
        readPageCounter = 0;
        writePageCounter = 0;
        appendPageCounter = 0;
        dataPageCount = 0;
        file = f;
    }

    FileHandle::~FileHandle() {
    }

    RC FileHandle::readPage(PageNum pageNum, void *data) {
        if (getNumberOfPages() < pageNum) return -1;
        fseek(file, (long) (1 + pageNum) * PAGE_SIZE, SEEK_SET);
        fread(data, PAGE_SIZE, 1, file);
        this->readPageCounter++;
        return 0;
    }

    RC FileHandle::writePage(PageNum pageNum, const void *data) {
        if (getNumberOfPages() < pageNum) return -1;
        fseek(file, (long) (1 + pageNum) * PAGE_SIZE, SEEK_SET);
        fwrite(data, PAGE_SIZE, 1, file);
        this->writePageCounter++;
        return 0;
    }

    RC FileHandle::appendPage(const void *data) {
        fseek(file, 0, SEEK_END);
        fwrite(data, PAGE_SIZE, 1, file);
        this->appendPageCounter++;
        this->pageSpaceMap[dataPageCount] = PAGE_SIZE;
        this->dataPageCount++;
        return 0;
    }

    unsigned FileHandle::getNumberOfPages() {
        return this->dataPageCount;
    }

    RC FileHandle::setPageSpace(PageNum num, short freeBytes) {
        this->pageSpaceMap[num] = freeBytes;
    }

    RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        readPageCount = this->readPageCounter;
        writePageCount = this->writePageCounter;
        appendPageCount = this->appendPageCounter;
        return 0;
    }

    RC FileHandle::openFile() {
        unsigned counters[4] = { 0, 0, 0, 0 };
        fseek(file, 0, SEEK_SET);
        fread(counters, sizeof(unsigned), 4, file);
        this->readPageCounter = counters[0];
        this->writePageCounter = counters[1];
        this->appendPageCounter = counters[2];
        this->dataPageCount = counters[3];
        fread(&this->pageSpaceMap, sizeof(PageNum) + sizeof(short), dataPageCount, file);
        return 0;
    }

    RC FileHandle::closeFile() {
        persistCounters();
        return 0;
    }

    bool FileHandle::isOpen() const {
        return file != NULL;
    }

    RC FileHandle::init(FILE *file) {
        fseek(file, 0, SEEK_SET);
        unsigned counters[PAGE_SIZE / sizeof(unsigned)] = { 0, 0, 1, 0 }; // Initialize appendPageCounter to 1 since we are using a hidden page
        fwrite(counters, sizeof(unsigned), PAGE_SIZE / sizeof(unsigned), file);
        return 0;
    }

    bool FileHandle::exists(const std::string &fileName) {
        if(fopen(fileName.c_str(), "r")) return true;
        return false;
    }

    RC FileHandle::persistCounters(){
        if (NULL == file) return 0;
        fseek(file, 0, SEEK_SET);
        this->writePageCounter++;
        unsigned counters[4] = { readPageCounter, writePageCounter, appendPageCounter, dataPageCount };
        fwrite(counters, sizeof(unsigned), 4, file);
        fwrite(&pageSpaceMap, (sizeof(PageNum) + sizeof(short)), pageSpaceMap.size(), file); //TODO: Handle when map becomes too big for one page
        return 0;
    }

    short FileHandle::findFreePage(size_t bytesToStore) {
        for (int i = (int)pageSpaceMap.size() - 1; i >= 0 ; --i){
            if (pageSpaceMap[i] > bytesToStore)
                return i;
        }
        return -1;
    }

} // namespace PeterDB
