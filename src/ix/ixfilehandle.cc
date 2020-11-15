#include "src/include/ix.h"

namespace PeterDB {

    IXFileHandle::IXFileHandle() {
        ixReadPageCounter = 0;
        ixWritePageCounter = 0;
        ixAppendPageCounter = 0;
        rootPage = -1;
    }

    IXFileHandle::~IXFileHandle() = default;

    IXFileHandle& IXFileHandle::operator=(const IXFileHandle &other) {
        this->ixReadPageCounter = other.ixReadPageCounter;
        this->ixWritePageCounter = other.ixWritePageCounter;
        this->ixAppendPageCounter = other.ixAppendPageCounter;
        this->rootPage = other.rootPage;
        // use setFile for fstream ixFile
        return *this;
    }

    void IXFileHandle::setFile(std::fstream &&file) {
        ixFile = std::move(file);
    }

    RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        readPageCount = ixReadPageCounter,
        writePageCount = ixWritePageCounter,
        appendPageCount = ixAppendPageCounter;
        return 0;
    }

    RC IXFileHandle::create(const std::string &fileName) {
        if (FileHandle::exists(fileName)) return -1;
        ixFile = fstream(fileName, ios::out);
        init();
        close();
        return 0;
    }

    void IXFileHandle::init() {
        ixReadPageCounter = 0,
        ixWritePageCounter = 0,
        ixAppendPageCounter = 1,
        rootPage = -1;
    }

    RC IXFileHandle::open(const std::string &fileName) {
        if (ixFile.is_open() || !FileHandle::exists(fileName)) {
            return -1;
        }
        ixFile = fstream(fileName, ios::in | ios::out | ios::binary);

        int counters[4] = { 0, 0, 0, 0 };
        ixFile.seekg(0);
        ixFile.read(reinterpret_cast<char *>(counters), sizeof(counters));
        ixReadPageCounter = counters[0];
        ixWritePageCounter = counters[1];
        ixAppendPageCounter = counters[2];
        rootPage = counters[3];
        ixReadPageCounter++;

        return 0;
    }

    RC IXFileHandle::close() {
        if (!ixFile.is_open())
            return 0;

        ixWritePageCounter++;
        ixFile.seekp(0);
        int counters[4] = { static_cast<int>(ixReadPageCounter), static_cast<int>(ixWritePageCounter), static_cast<int>(ixAppendPageCounter), rootPage };
        ixFile.write(reinterpret_cast<char *>(counters), sizeof(counters));
        int spaceToReserve = PAGE_SIZE * IX_HIDDEN_PAGE_COUNT - sizeof(counters);
        char junk[spaceToReserve];
        ixFile.write(junk, spaceToReserve);

        ixFile.close();
        return 0;
    }

    RC IXFileHandle::readPage(PageNum pageNum, void *data) {
        char bytes[PAGE_SIZE];
        if (ixFile.eof())
            ixFile.clear();
        ixFile.seekg(pageNum * PAGE_SIZE, ios::beg);
        ixFile.read(bytes, PAGE_SIZE);
        std::memcpy(data, bytes, PAGE_SIZE);
        ixReadPageCounter++;
        return 0;
    }

    RC IXFileHandle::writePage(PageNum pageNum, const void *data) {
        ixFile.seekp(pageNum * PAGE_SIZE, ios::beg);
        ixFile.write(reinterpret_cast<char *>(const_cast<void *>(data)), PAGE_SIZE);
        ixFile.flush();
        ixWritePageCounter++;
        return 0;
    }

    int IXFileHandle::appendPage(const void *data) {
        ixFile.seekp(ixAppendPageCounter * PAGE_SIZE, ios::beg);
        ixFile.write(reinterpret_cast<char *>(const_cast<void *>(data)), PAGE_SIZE);
        ixFile.flush();
        ixAppendPageCounter++;
        return static_cast<int>(ixAppendPageCounter);
    }
}