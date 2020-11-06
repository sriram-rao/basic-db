#ifndef PETERDB_COPY_UTILS_H
#define PETERDB_COPY_UTILS_H

#include <cstring>

using namespace std;

namespace PeterDB {
    class CopyUtils {
    public:
        static int copyAttribute(const void *data, void* destination, int& sourceOffset, int& destOffset, int length) {
            memcpy((char*) destination + destOffset, (char *) data + sourceOffset, length);
            sourceOffset += length;
            destOffset += length;
            return sourceOffset;
        }

        static int copyAttribute(const void *data, void* destination, int& startOffset, int length) {
            std::memcpy(destination, (char *) data + startOffset, length);
            startOffset += length;
            return startOffset;
        }
    };
}


#endif //PETERDB_COPY_UTILS_H
