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
        if (FileHandle::fileExists(fileName)) return -1;
        FILE* newFile;
        newFile = fopen(fileName.c_str(), "w");
        fclose(newFile);
        return 0;
    }

    RC PagedFileManager::destroyFile(const std::string &fileName) {
        if(!FileHandle::fileExists(fileName)) return -1;
        remove(fileName.c_str());
        return 0;
    }

    RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        if (fileHandle.checkIfOpen()) return -1;
        FILE* f = fopen(fileName.c_str(), "w");
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
        return -1;
    }

    RC FileHandle::writePage(PageNum pageNum, const void *data) {
        return -1;
    }

    RC FileHandle::appendPage(const void *data) {
        return -1;
    }

    unsigned FileHandle::getNumberOfPages() {
        return -1;
    }

    RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        return -1;
    }

    RC FileHandle::openFile() {
        // Load the counters
        return 0;
    }

    RC FileHandle::closeFile() {
        persistCounters();
        return 0;
    }

    bool FileHandle::checkIfOpen(){
        return file != NULL;
    }

    bool FileHandle::fileExists(const std::string &fileName) {
        if(fopen(fileName.c_str(), "r")) return true;
        return false;
    }

    RC FileHandle::persistCounters(){
        return -1;
    }

} // namespace PeterDB