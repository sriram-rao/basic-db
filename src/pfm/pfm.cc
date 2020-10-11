#include "src/include/pfm.h"
#include <cstdio>
#include <fstream>

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
        newFile = fopen(fileName.c_str(), "w");
        fclose(newFile);
        return 0;
    }

    RC PagedFileManager::destroyFile(const std::string &fileName) {
        if(!FileHandle::exists(fileName)) return -1;
        remove(fileName.c_str());
        return 0;
    }

    RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        if (fileHandle.isOpen()) return -1;
        FILE* f = fopen(fileName.c_str(), "w+b");
        fileHandle = * (new FileHandle(f));
        fileHandle.openFile();
        return 0;
    }

    RC PagedFileManager::closeFile(FileHandle &fileHandle) {
        fileHandle.closeFile();
        fclose(fileHandle.file);
    }

    FileHandle::FileHandle() {
        readPageCounter = 0;
        writePageCounter = 0;
        appendPageCounter = 0;
        file = NULL;
    }

    FileHandle::FileHandle(FILE* f): file(f) {
    }

    FileHandle::~FileHandle() {
        persistCounters();
    }


    FileHandle &FileHandle::operator=(const FileHandle &that) {
        this->persistCounters();
        this->readPageCounter = that.readPageCounter;
        this->writePageCounter = that.writePageCounter;
        this->appendPageCounter = that.appendPageCounter;
        this->file = that.file;
        return *this;
    }

    RC FileHandle::readPage(PageNum pageNum, void *data) {
        if (getNumberOfPages() <= pageNum) return -1;
        fseek(file, (long) (1 + pageNum) * PAGE_SIZE, SEEK_SET);
        int readSize = fread(data, 1, PAGE_SIZE, file);
        if (readSize != PAGE_SIZE) return -1;
        readPageCounter++;
        return 0;
    }

    RC FileHandle::writePage(PageNum pageNum, const void *data) {
        if (getNumberOfPages() <= pageNum) return -1;
        fseek(file, (long) (1 + pageNum) * PAGE_SIZE, SEEK_SET);
        int writeSize = fwrite(data, 1, PAGE_SIZE, file);
        if (writeSize != PAGE_SIZE) return -1;
        writePageCounter++;
        return 0;
    }

    RC FileHandle::appendPage(const void *data) {
        fseek(file, 0, SEEK_END);
        fwrite(data, 1, PAGE_SIZE, file);
        appendPageCounter++;
        return 0;
    }

    unsigned FileHandle::getNumberOfPages() {
        return appendPageCounter;
    }

    RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        readPageCount = this->readPageCounter;
        writePageCount = this->writePageCounter;
        appendPageCount = this->appendPageCounter;
        return 0;
    }

    RC FileHandle::openFile() {
        // Load the counters
        unsigned counters[3];
        fseek(file, 0, SEEK_SET);
        fread((void*) counters, sizeof(unsigned), 3, file);
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

    bool FileHandle::exists(const std::string &fileName) {
        if(fopen(fileName.c_str(), "r")) return true;
        return false;
    }

    RC FileHandle::persistCounters(){
        fseek(file, 0, SEEK_SET);
        unsigned int counters[3] = { readPageCounter, writePageCounter, appendPageCounter };
        fwrite(counters, sizeof(unsigned), 3, file);
        return 0;
    }

} // namespace PeterDB