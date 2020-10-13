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
        file = NULL;
    }

    FileHandle::FileHandle(FILE* f) {
        readPageCounter = 0;
        writePageCounter = 0;
        appendPageCounter = 0;
        file = f;
    }

    FileHandle::~FileHandle() {
    }

    RC FileHandle::readPage(PageNum pageNum, void *data) {
        if (getNumberOfPages() < pageNum) return -1;
        fseek(file, (long) (1 + pageNum) * PAGE_SIZE, SEEK_SET);
        fread(data, PAGE_SIZE, 1, file);
        this->readPageCounter = this->readPageCounter + 1;
        return 0;
    }

    RC FileHandle::writePage(PageNum pageNum, const void *data) {
        if (getNumberOfPages() < pageNum) return -1;
        fseek(file, (long) (1 + pageNum) * PAGE_SIZE, SEEK_SET);
        fwrite(data, PAGE_SIZE, 1, file);
        this->writePageCounter = this->writePageCounter + 1;
        return 0;
    }

    RC FileHandle::appendPage(const void *data) {
        fseek(file, 0, SEEK_END);
        fwrite(data, PAGE_SIZE, 1, file);
        this->appendPageCounter = this->appendPageCounter + 1;
        return 0;
    }

    unsigned FileHandle::getNumberOfPages() {
        return this->appendPageCounter;
    }

    RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        readPageCount = this->readPageCounter;
        writePageCount = this->writePageCounter;
        appendPageCount = this->appendPageCounter;
        return 0;
    }

    RC FileHandle::openFile() {
        unsigned counters[3] = { 0, 0, 0 };
        fseek(file, 0, SEEK_SET);
        fread(counters, sizeof(unsigned), 3, file);
        this->readPageCounter = counters[0];
        this->writePageCounter = counters[1];
        this->appendPageCounter = counters[2];
        return 0;
    }

    RC FileHandle::closeFile() {
        persistCounters();
        return 0;
    }

    bool FileHandle::isOpen() const{
        return file != NULL;
    }

    RC FileHandle::init(FILE *file) {
        fseek(file, 0, SEEK_SET);
        unsigned counters[PAGE_SIZE / sizeof(unsigned)] = {0, 0, 0};
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
        unsigned counters[3] = { readPageCounter, writePageCounter, appendPageCounter };
        fwrite(counters, sizeof(unsigned), 3, file);
        return 0;
    }

} // namespace PeterDB