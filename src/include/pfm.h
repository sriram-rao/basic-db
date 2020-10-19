#ifndef _pfm_h_
#define _pfm_h_

#define PAGE_SIZE 4096

#include <string>
#include <unordered_map>
#include <vector>
#include <fstream>

namespace PeterDB {

    typedef unsigned PageNum;
    typedef int RC;

    class FileHandle;

    class PagedFileManager {
    public:
        static PagedFileManager &instance();                                // Access to the singleton instance

        RC createFile(const std::string &fileName);                         // Create a new file
        RC destroyFile(const std::string &fileName);                        // Destroy a file
        RC openFile(const std::string &fileName, FileHandle &fileHandle);   // Open a file
        RC closeFile(FileHandle &fileHandle);                               // Close a file

    protected:
        PagedFileManager();                                                 // Prevent construction
        ~PagedFileManager();                                                // Prevent unwanted destruction
        PagedFileManager(const PagedFileManager &);                         // Prevent construction by copying
        PagedFileManager &operator=(const PagedFileManager &);              // Prevent assignment

    };

    class FileHandle {
    public:
        // variables to keep the counter for each operation
        unsigned readPageCounter;
        unsigned writePageCounter;
        unsigned appendPageCounter;
        std::vector<short> pageSpaceMap;
        std::fstream file;

        FileHandle();                                                       // Default constructor
        ~FileHandle();                                                      // Destructor
        explicit FileHandle(std::fstream&& file);
        FileHandle& operator= (const FileHandle& other);

        RC readPage(PageNum pageNum, void *data);                           // Get a specific page
        RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
        RC appendPage(const void *data);                                    // Append a specific page
        unsigned getNumberOfPages();                                        // Get the number of pages in the file
        RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount,
                                unsigned &appendPageCount);                 // Put current counter values into variables
        RC open();
        RC close();
        void setFile(std::fstream&& fileToSet);
        bool isOpen() const;
        RC setPageSpace(PageNum num, short freeBytes);
        static bool exists(const std::string &fileName);
        RC init();                                         // Initialize a new file with hidden pages

        short findFreePage(size_t i);

    private:
        RC persistCounters();
    };
} // namespace PeterDB

#endif // _pfm_h_