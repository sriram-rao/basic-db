#include "../include/page.h"
#include<vector>


namespace PeterDB {
    Page::Page() { }
    Page::Page(SlotDirectory directory, vector<Record> records) {
        this->directory = directory;
        this->records = records;
    }
    Page::~Page() = default;
}
