#ifndef SSD_SHUF_BUCKET_H
#define SSD_SHUF_BUCKET_H

#include <chrono>
#include <cstring>
#include <fstream>
#include <iostream>
#include <iterator>
#include <vector>
#include <type_traits>
#include <stdint.h>
#include <limits.h>
#include <algorithm>
#include <stdio.h>
#include <sys/resource.h>
#include <unistd.h>

using namespace std;

template <size_t bufSize>
struct Bucket {
    Bucket(): buf(bufSize), end(0), fileOffset(0) {
        char tmplLines[PATH_MAX] = "tmp/lines_XXXXXX";
        char tmplOffsets[PATH_MAX] = "tmp/offsets_XXXXXX";
        int descr = mkstemp(tmplLines);
        fLinesName = tmplLines;
        fLines = fdopen(descr, "wb+");
        if (!fLines) {
            throw runtime_error("cannot open bucket file");
        }
        descr = mkstemp(tmplOffsets);
        fOffsetsName = tmplOffsets;
        fOffsets = fdopen(descr, "wb+");
        if (!fOffsets) {
            throw runtime_error("cannot open bucket file");
        }
    }

    void append(const uint8_t *line, size_t len) {
        if(len > numeric_limits<lengths_type>::max() ||
           len > bufSize) {
            //write directly
            fileOffset+=len;
            fwrite(&fileOffset, sizeof(uint32_t), 1, fOffsets);
            fwrite(line, 1, len, fLines);
        } else {
            if (end+len > bufSize) {
                flush();
            }
            memcpy(buf.data() + end, line, len);
            lengths.push_back(len);
            end += len;
        }
    }

    void flush() {
        vector<uint32_t> offsets(0);
        transform(lengths.begin(), lengths.end(), std::back_inserter(offsets),
                [this](lengths_type len) -> uint32_t { return fileOffset+=len; } );
        fwrite(buf.data(), 1, end, fLines);
        fwrite(offsets.data(), sizeof(uint32_t), offsets.size(), fOffsets);
        end = 0;
        lengths.clear();
    }

    void stopFill() {
        flush();
        buf.clear();
        buf.shrink_to_fit();
        lengths.clear();
        lengths.shrink_to_fit();
    }

    size_t shuffleAndWrite(FILE *fOut) {
        size_t linesSize = ftell(fLines);
        size_t offsetsSize = ftell(fOffsets);
        fseek(fLines, 0, SEEK_SET);
        fseek(fOffsets, 0, SEEK_SET);

        vector<uint8_t> lines(linesSize);
        vector<uint32_t> offsets(1 + offsetsSize/4);
        offsets[0] = 0;
        if(lines.size() != fread(lines.data(), 1, linesSize, fLines))
            throw runtime_error("wrong lines size");
        if(offsets.size() - 1 != fread(offsets.data() + 1, 4, offsets.size() - 1, fOffsets))
            throw runtime_error("wrong offsets size");

        vector<uint32_t> idx(offsets.size()-1);
        iota(idx.begin(), idx.end(), 0);

        std::random_device rd;
        std::mt19937_64 g(rd());

        std::shuffle(idx.begin(), idx.end(), g);

        const size_t writeSize = 32*1024;
        uint8_t writeBuf[writeSize];
        size_t writePos = 0;
        for(auto i:idx) {
            uint8_t *offset = lines.data() + offsets[i];
            size_t len = offsets[i+1] - offsets[i];
            if(len + 1 > writeSize - writePos) {
                //flush
                fwrite(writeBuf, 1, writePos, fOut);
                writePos = 0;
            }
            if(len + 1 > writeSize/2) {
                uint8_t newline = '\n';
                fwrite(offset, 1, len, fOut);
                fwrite(&newline, 1, 1, fOut);
            } else {
                memcpy(writeBuf+writePos, offset, len);
                writeBuf[writePos+len] = '\n';
                writePos += len + 1;
            }
        }
        //flush
        fwrite(writeBuf, 1, writePos, fOut);
        return linesSize + offsetsSize/4;
    }

    ~Bucket() {
        if(fOffsets) {
            fclose(fOffsets);
            fOffsets = 0;
            unlink(fOffsetsName.c_str());
        }
        if(fLines) {
            fclose(fLines);
            fLines = 0;
            unlink(fLinesName.c_str());
        }
    }

    FILE* fLines;
    string fLinesName;
    FILE* fOffsets;
    string fOffsetsName;
    size_t end;
    uint32_t fileOffset;
    vector<uint8_t> buf; //combine lengths and buf to fix the memory usage
    typedef uint16_t lengths_type;
    vector<lengths_type> lengths; //each string length
};


#endif //SSD_SHUF_BUCKET_H
