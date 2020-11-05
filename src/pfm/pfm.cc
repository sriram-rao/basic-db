#include "src/include/pfm.h"
#include <cstdio>
#include <vector>
#include <cstring>
#include <fstream>

using namespace std;

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
        fstream newFile(fileName, ios::out);
        FileHandle handle(std::move(newFile));
        handle.init();
        handle.close();
        return 0;
    }

    RC PagedFileManager::destroyFile(const std::string &fileName) {
        return remove(fileName.c_str());
    }

    RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        if (fileHandle.isOpen()) return -1;
        bool newFile = !FileHandle::exists(fileName);
        fstream file(fileName, ios::in | ios::out | ios::binary);
        fileHandle.setFile(std::move(file));
        if (newFile)
            return fileHandle.init();
        return fileHandle.open();
    }

    RC PagedFileManager::closeFile(FileHandle &fileHandle) {
        fileHandle.close();
        return 0;
    }

    FileHandle::FileHandle() {
        readPageCounter = 0;
        writePageCounter = 0;
        appendPageCounter = 0;
    }

    FileHandle::FileHandle(fstream&& file) {
        readPageCounter = 0;
        writePageCounter = 0;
        appendPageCounter = 0;
        this->file = std::move(file);
    }

    FileHandle& FileHandle::operator= (const FileHandle& other) {
        this->readPageCounter = other.readPageCounter;
        this->writePageCounter = other.writePageCounter;
        this->appendPageCounter = other.appendPageCounter;
        this->pageSpaceMap = other.pageSpaceMap;
        // Copy assignment will copy the state of this class except the filestream itself. Use setFile to move the file stream.
        // this->file = std::move(other.file);
        return *this;
    }

    FileHandle::~FileHandle() = default;

    RC FileHandle::readPage(PageNum pageNum, void *data) {
        if (getNumberOfPages() < pageNum) return -1;
        char bytes[PAGE_SIZE];
        if (file.eof())
            file.clear();
        file.seekg((HIDDEN_PAGE_COUNT + pageNum) * PAGE_SIZE, ios::beg);
        file.read(bytes, PAGE_SIZE);
        std::memcpy(data, bytes, PAGE_SIZE);
        readPageCounter++;
        return 0;
    }

    RC FileHandle::writePage(PageNum pageNum, const void *data) {
        if (getNumberOfPages() < pageNum) return -1;
        file.seekp((HIDDEN_PAGE_COUNT + pageNum) * PAGE_SIZE, ios::beg);
        file.write(reinterpret_cast<char *>(const_cast<void *>(data)), PAGE_SIZE);
        file.flush();
        writePageCounter++;
        return 0;
    }

    RC FileHandle::appendPage(const void *data) {
        appendPageCounter++;
        pageSpaceMap.push_back(PAGE_SIZE);
        file.seekp((getNumberOfPages() + HIDDEN_PAGE_COUNT - 1) * PAGE_SIZE, ios::beg);
        file.write(reinterpret_cast<char *>(const_cast<void *>(data)), PAGE_SIZE);
        file.flush();
        return 0;
    }

    unsigned FileHandle::getNumberOfPages() {
        return pageSpaceMap.size();
    }

    RC FileHandle::setPageSpace(PageNum num, short freeBytes) {
        this->pageSpaceMap[num] = freeBytes;
        persistCounters();
        return 0;
    }

    RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        readPageCount = this->readPageCounter;
        writePageCount = this->writePageCounter;
        appendPageCount = this->appendPageCounter;
        return 0;
    }

    RC FileHandle::open() {
        unsigned counters[4] = { 0, 0, 0, 0 };
        file.seekg(0);
        file.read(reinterpret_cast<char *>(counters), sizeof(counters));
        this->readPageCounter = counters[0];
        this->writePageCounter = counters[1];
        this->appendPageCounter = counters[2];
        unsigned dataPageCount = counters[3];
        if (dataPageCount > 0) {
            pageSpaceMap.clear();
            pageSpaceMap.insert(pageSpaceMap.begin(), dataPageCount, PAGE_SIZE);
            file.read(reinterpret_cast<char *>(pageSpaceMap.data()), sizeof(short) * dataPageCount);
        }
        this->readPageCounter++;
        return 0;
    }

    void FileHandle::setFile(std::fstream &&fileToSet) {
        this->file = std::move(fileToSet);
    }

    RC FileHandle::close() {
        if (!file.is_open())
            return 0;
        persistCounters();
        file.close();
        return 0;
    }

    bool FileHandle::isOpen() const {
        return file.is_open();
    }

    RC FileHandle::init() {
        // Initialize appendPageCounter to 1 since we are using a hidden page
        readPageCounter = 0, writePageCounter = 0, appendPageCounter = 1;
        return 0;
    }

    bool FileHandle::exists(const std::string &fileName) {
        if (FILE *file = fopen(fileName.c_str(), "r")) {
            fclose(file);
            return true;
        }
        return false;
    }

    RC FileHandle::persistCounters(){
        writePageCounter++;
        file.seekp(0);
        unsigned counters[4] = { readPageCounter, writePageCounter, appendPageCounter, static_cast<unsigned>(pageSpaceMap.size()) };
        file.write(reinterpret_cast<char *>(counters), sizeof(counters));
        if (!pageSpaceMap.empty())
            file.write(reinterpret_cast<char *>(pageSpaceMap.data()), sizeof(short) * pageSpaceMap.size());  //TODO: Handle when map becomes too big for one page
        int spaceToReserve = PAGE_SIZE * HIDDEN_PAGE_COUNT - sizeof(counters) - sizeof(short) * pageSpaceMap.size();
        char junk[spaceToReserve];
        file.write(junk, spaceToReserve);
        file.flush();
        return 0;
    }

    int16_t FileHandle::findFreePage(short bytesToStore) {
        open();
        for (int i = (int) getNumberOfPages() - 1; i >= 0 ; --i){
            if (pageSpaceMap[i] > bytesToStore)
                return static_cast<int16_t>(i);
        }
        return -1;
    }

} // namespace PeterDB
